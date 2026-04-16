use crate::{
    discovery::upsert_wallet_activity_days_on_conn, DiscoveryPersistedRebuildPhase,
    DiscoveryPersistedRebuildRowDriverCompareDiagnostic,
    DiscoveryPersistedRebuildRowDriverCompareOptions,
    DiscoveryPersistedRebuildRowDriverCompareStage,
    DiscoveryPersistedRebuildRowSharedPathDiffDiagnostic,
    DiscoveryPersistedRebuildRowSharedPathDiffOptions,
    DiscoveryPersistedRebuildRowSharedPathDiffStage,
    DiscoveryPersistedRebuildRowSharedSequenceCompareDiagnostic,
    DiscoveryPersistedRebuildRowSharedSequenceCompareOptions,
    DiscoveryPersistedRebuildRowSharedSequenceCompareStage,
    DiscoveryPersistedRebuildRowStepMetaCompareDiagnostic,
    DiscoveryPersistedRebuildRowStepMetaCompareOptions,
    DiscoveryPersistedRebuildRowStepMetaCompareStage,
    DiscoveryPersistedRebuildRowStepMetaIsolatedSharedDiagnostic,
    DiscoveryPersistedRebuildRowStepMetaIsolatedSharedOptions,
    DiscoveryPersistedRebuildRowStepMetaIsolatedSharedStage,
    DiscoveryPersistedRebuildStateMetaLiteRawRow, DiscoveryPersistedRebuildStateMetaRow,
    DiscoveryPersistedRebuildStateRow, DiscoveryRuntimeCursor, ObservedSwapBatchWriteMetrics,
    ObservedSwapsCoverageSnapshot, RecentRawJournalReplaySummary, RecentRawJournalStateRow,
    RecentRawJournalWriteSummary, SqliteBatchedDeleteSummary, SqliteStore, TokenMarketStats,
    TokenQualityCacheRow, TokenQualityRpcRow, WalletActivityDayRow, WalletRecentActivityCountRow,
};
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use reqwest::blocking::Client;
use rusqlite::{
    params, params_from_iter, types::Value as SqlValue, Connection, ErrorCode, OptionalExtension,
};
use serde_json::{json, Value};
#[cfg(test)]
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::mpsc;
use std::time::{Duration as StdDuration, Instant};

const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const OBSERVED_SWAP_CURSOR_PROGRESS_OPS: i32 = 2_000;
const OBSERVED_SWAP_CURSOR_QUERY_PAGE_LIMIT: usize = 2_048;
const OBSERVED_SOL_LEG_CURSOR_QUERY_PAGE_LIMIT: usize = 2_048;
const OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE: &str = "temp_discovery_replay_target_buy_mints";
const OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_META_TABLE: &str =
    "temp_discovery_replay_target_buy_mints_meta";
const OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_TABLE: &str =
    "temp_discovery_replay_candidate_wallets";
const OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_META_TABLE: &str =
    "temp_discovery_replay_candidate_wallets_meta";
const RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW: usize = 13;
const RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS: usize = 512;

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

#[derive(Debug)]
struct PersistedRebuildRowQueryPlusNextWithExtractsResult {
    prepare_meta_elapsed_ms: u64,
    step_meta_elapsed_ms: u64,
    extract_phase_elapsed_ms: u64,
    extract_updated_at_elapsed_ms: u64,
    total_elapsed_ms: u64,
    row_exists: bool,
    row_phase: Option<String>,
    row_updated_at: Option<String>,
}

#[derive(Debug)]
struct PersistedRebuildRowQueryPlusNextStepOnlyResult {
    step_meta_elapsed_ms: u64,
    row_exists: bool,
}

#[derive(Debug)]
struct QueryRowVariantResult {
    total_elapsed_ms: u64,
}

#[derive(Debug)]
struct PersistedRebuildRowExistsProbeResult {
    prepare_exists_elapsed_ms: u64,
    step_exists_elapsed_ms: u64,
    row_exists: bool,
}

#[derive(Debug)]
struct PersistedRebuildRowStepMetaSharedPathResult {
    connection_facts: crate::SqliteReadOnlyDriverCompareFacts,
    exists_probe: PersistedRebuildRowExistsProbeResult,
    query_result: PersistedRebuildRowQueryPlusNextWithExtractsResult,
    loads_connection_facts_before_meta_query: bool,
    uses_query_plus_next: bool,
    finalizes_exists_before_prepare_meta: bool,
    extracts_phase_and_updated_at_after_step: bool,
    measures_prepare_meta_separately: bool,
    measures_extract_separately: bool,
}

#[derive(Debug)]
struct PersistedRebuildRowSharedSequenceBaselinePathResult {
    connection_facts: crate::SqliteReadOnlyDriverCompareFacts,
    exists_probe: PersistedRebuildRowExistsProbeResult,
    query_result: PersistedRebuildRowQueryPlusNextStepOnlyResult,
    loads_connection_facts_before_meta_query: bool,
    uses_query_plus_next: bool,
    finalizes_exists_before_prepare_meta: bool,
    extracts_phase_and_updated_at_after_step: bool,
    measures_prepare_meta_separately: bool,
    measures_extract_separately: bool,
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
struct ObservedWalletActivityWalletIdPage {
    wallet_ids: Vec<String>,
    time_budget_exhausted: bool,
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

#[cfg(test)]
thread_local! {
    static RECENT_RAW_BULK_WRITE_BUDGET_HOOK: RefCell<Option<Box<dyn FnMut(usize, usize) -> bool>>> =
        RefCell::new(None);
}

#[cfg(test)]
struct RecentRawBulkWriteBudgetHookGuard;

#[cfg(test)]
impl Drop for RecentRawBulkWriteBudgetHookGuard {
    fn drop(&mut self) {
        RECENT_RAW_BULK_WRITE_BUDGET_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

#[cfg(test)]
fn install_recent_raw_bulk_write_budget_hook<F>(hook: F) -> RecentRawBulkWriteBudgetHookGuard
where
    F: FnMut(usize, usize) -> bool + 'static,
{
    RECENT_RAW_BULK_WRITE_BUDGET_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "recent_raw bulk write hook already installed"
        );
        *slot = Some(Box::new(hook));
    });
    RecentRawBulkWriteBudgetHookGuard
}

#[cfg(test)]
fn recent_raw_bulk_write_test_hook_requests_budget_exhaustion(
    processed_rows: usize,
    inserted_rows: usize,
) -> bool {
    RECENT_RAW_BULK_WRITE_BUDGET_HOOK.with(|slot| {
        slot.borrow_mut()
            .as_mut()
            .is_some_and(|hook| hook(processed_rows, inserted_rows))
    })
}

#[cfg(not(test))]
fn recent_raw_bulk_write_test_hook_requests_budget_exhaustion(
    _processed_rows: usize,
    _inserted_rows: usize,
) -> bool {
    false
}

fn parse_rfc3339_utc(raw: &str, field_name: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid {field_name} timestamp value: {raw}"))
}

fn parse_optional_rfc3339_utc(
    raw: Option<String>,
    field_name: &str,
) -> Result<Option<DateTime<Utc>>> {
    raw.map(|raw| parse_rfc3339_utc(&raw, field_name))
        .transpose()
}

fn discovery_runtime_cursor_cmp(
    left: &DiscoveryRuntimeCursor,
    right: &DiscoveryRuntimeCursor,
) -> Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

fn ensure_recent_raw_journal_tables_on_conn(conn: &Connection) -> Result<()> {
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
        CREATE TABLE IF NOT EXISTS recent_raw_journal_state (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            covered_since_ts TEXT,
            covered_through_cursor_ts TEXT,
            covered_through_cursor_slot INTEGER,
            covered_through_cursor_signature TEXT,
            row_count INTEGER NOT NULL DEFAULT 0,
            last_batch_rows INTEGER NOT NULL DEFAULT 0,
            last_batch_completed_at TEXT,
            last_pruned_rows INTEGER NOT NULL DEFAULT 0,
            last_pruned_at TEXT,
            updated_at TEXT NOT NULL
        );",
    )
    .context("failed ensuring recent raw journal tables exist")
}

fn recent_raw_journal_bulk_insert_sql(row_count: usize) -> String {
    let row_placeholders = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    let placeholders = std::iter::repeat_n(row_placeholders, row_count)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
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
         ) VALUES {placeholders}"
    )
}

fn recent_raw_journal_effective_bulk_insert_chunk_rows(
    requested_chunk_rows: Option<usize>,
    sqlite_variable_limit: usize,
) -> usize {
    let requested_or_default =
        requested_chunk_rows.unwrap_or(RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS);
    let sqlite_limit_rows = sqlite_variable_limit / RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW;
    requested_or_default
        .min(RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS)
        .min(sqlite_limit_rows)
        .max(1)
}

fn recent_raw_journal_sqlite_variable_limit(conn: &Connection) -> usize {
    unsafe {
        rusqlite::ffi::sqlite3_limit(
            conn.handle(),
            rusqlite::ffi::SQLITE_LIMIT_VARIABLE_NUMBER,
            -1,
        )
    }
    .max(0) as usize
}

fn recent_raw_elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

fn push_recent_raw_journal_bulk_insert_values(values: &mut Vec<SqlValue>, swap: &SwapEvent) {
    values.push(SqlValue::Text(swap.signature.clone()));
    values.push(SqlValue::Text(swap.wallet.clone()));
    values.push(SqlValue::Text(swap.dex.clone()));
    values.push(SqlValue::Text(swap.token_in.clone()));
    values.push(SqlValue::Text(swap.token_out.clone()));
    values.push(SqlValue::Real(swap.amount_in));
    values.push(SqlValue::Real(swap.amount_out));
    values.push(
        swap.exact_amounts
            .as_ref()
            .map(|value| SqlValue::Text(value.amount_in_raw.clone()))
            .unwrap_or(SqlValue::Null),
    );
    values.push(
        swap.exact_amounts
            .as_ref()
            .map(|value| SqlValue::Integer(i64::from(value.amount_in_decimals)))
            .unwrap_or(SqlValue::Null),
    );
    values.push(
        swap.exact_amounts
            .as_ref()
            .map(|value| SqlValue::Text(value.amount_out_raw.clone()))
            .unwrap_or(SqlValue::Null),
    );
    values.push(
        swap.exact_amounts
            .as_ref()
            .map(|value| SqlValue::Integer(i64::from(value.amount_out_decimals)))
            .unwrap_or(SqlValue::Null),
    );
    values.push(SqlValue::Integer(swap.slot as i64));
    values.push(SqlValue::Text(swap.ts_utc.to_rfc3339()));
}

fn recent_raw_journal_sqlite_error_is_operation_interrupted(error: &rusqlite::Error) -> bool {
    error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted)
}

fn recent_raw_journal_anyhow_error_is_operation_interrupted(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<rusqlite::Error>()
            .is_some_and(recent_raw_journal_sqlite_error_is_operation_interrupted)
            || cause
                .to_string()
                .to_ascii_lowercase()
                .contains("interrupted")
    })
}

fn recent_raw_journal_coverage_snapshot_on_conn(
    conn: &Connection,
) -> Result<(usize, Option<DateTime<Utc>>, Option<DiscoveryRuntimeCursor>)> {
    let row_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM observed_swaps", [], |row| row.get(0))
        .context("failed counting recent raw journal observed_swaps rows")?;
    let covered_since_raw: Option<String> = conn
        .query_row("SELECT MIN(ts) FROM observed_swaps", [], |row| row.get(0))
        .optional()
        .context("failed loading recent raw journal covered_since timestamp")?
        .flatten();
    let covered_since = parse_optional_rfc3339_utc(
        covered_since_raw,
        "recent_raw_journal_state.covered_since_ts",
    )?;
    let covered_through_cursor_raw = conn
        .query_row(
            "SELECT ts, slot, signature
             FROM observed_swaps
             ORDER BY ts DESC, slot DESC, signature DESC
             LIMIT 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get(2)?)),
        )
        .optional()
        .context("failed loading recent raw journal covered_through cursor")?;
    let covered_through_cursor = covered_through_cursor_raw
        .map(
            |(ts_raw, slot_raw, signature)| -> Result<DiscoveryRuntimeCursor> {
                Ok(DiscoveryRuntimeCursor {
                    ts_utc: parse_rfc3339_utc(&ts_raw, "observed_swaps.ts")?,
                    slot: slot_raw.max(0) as u64,
                    signature,
                })
            },
        )
        .transpose()?;
    Ok((
        row_count.max(0) as usize,
        covered_since,
        covered_through_cursor,
    ))
}

fn recent_raw_journal_state_query(conn: &Connection) -> Result<RecentRawJournalStateRow> {
    let (row_count, covered_since, covered_through_cursor) =
        recent_raw_journal_coverage_snapshot_on_conn(conn)?;
    let row = conn
        .query_row(
            "SELECT
                last_batch_rows,
                last_batch_completed_at,
                last_pruned_rows,
                last_pruned_at,
                updated_at
             FROM recent_raw_journal_state
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .optional()
        .context("failed reading recent raw journal state")?;
    let Some((
        last_batch_rows,
        last_batch_completed_at_raw,
        last_pruned_rows,
        last_pruned_at_raw,
        updated_at_raw,
    )) = row
    else {
        return Ok(RecentRawJournalStateRow {
            covered_since,
            covered_through_cursor,
            row_count,
            ..RecentRawJournalStateRow::default()
        });
    };
    Ok(RecentRawJournalStateRow {
        covered_since,
        covered_through_cursor,
        row_count,
        last_batch_rows: last_batch_rows.max(0) as usize,
        last_batch_completed_at: parse_optional_rfc3339_utc(
            last_batch_completed_at_raw,
            "recent_raw_journal_state.last_batch_completed_at",
        )?,
        last_pruned_rows: last_pruned_rows.max(0) as usize,
        last_pruned_at: parse_optional_rfc3339_utc(
            last_pruned_at_raw,
            "recent_raw_journal_state.last_pruned_at",
        )?,
        updated_at: parse_optional_rfc3339_utc(
            updated_at_raw,
            "recent_raw_journal_state.updated_at",
        )?,
    })
}

fn recent_raw_journal_state_cached_query(conn: &Connection) -> Result<RecentRawJournalStateRow> {
    let row = conn
        .query_row(
            "SELECT
                covered_since_ts,
                covered_through_cursor_ts,
                covered_through_cursor_slot,
                covered_through_cursor_signature,
                row_count,
                last_batch_rows,
                last_batch_completed_at,
                last_pruned_rows,
                last_pruned_at,
                updated_at
             FROM recent_raw_journal_state
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                ))
            },
        )
        .optional()
        .context("failed reading cached recent raw journal state")?;
    let Some((
        covered_since_raw,
        covered_through_ts_raw,
        covered_through_slot_raw,
        covered_through_signature,
        row_count,
        last_batch_rows,
        last_batch_completed_at_raw,
        last_pruned_rows,
        last_pruned_at_raw,
        updated_at_raw,
    )) = row
    else {
        return Ok(RecentRawJournalStateRow::default());
    };
    let covered_through_cursor = match (
        covered_through_ts_raw,
        covered_through_slot_raw,
        covered_through_signature,
    ) {
        (Some(ts_raw), Some(slot_raw), Some(signature)) => Some(DiscoveryRuntimeCursor {
            ts_utc: parse_rfc3339_utc(
                &ts_raw,
                "recent_raw_journal_state.covered_through_cursor_ts",
            )?,
            slot: slot_raw.max(0) as u64,
            signature,
        }),
        _ => None,
    };
    Ok(RecentRawJournalStateRow {
        covered_since: parse_optional_rfc3339_utc(
            covered_since_raw,
            "recent_raw_journal_state.covered_since_ts",
        )?,
        covered_through_cursor,
        row_count: row_count.max(0) as usize,
        last_batch_rows: last_batch_rows.max(0) as usize,
        last_batch_completed_at: parse_optional_rfc3339_utc(
            last_batch_completed_at_raw,
            "recent_raw_journal_state.last_batch_completed_at",
        )?,
        last_pruned_rows: last_pruned_rows.max(0) as usize,
        last_pruned_at: parse_optional_rfc3339_utc(
            last_pruned_at_raw,
            "recent_raw_journal_state.last_pruned_at",
        )?,
        updated_at: parse_optional_rfc3339_utc(
            updated_at_raw,
            "recent_raw_journal_state.updated_at",
        )?,
    })
}

fn recent_raw_journal_state_row_exists(conn: &Connection) -> Result<bool> {
    let row = conn
        .query_row(
            "SELECT 1
             FROM recent_raw_journal_state
             WHERE id = 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .context("failed checking cached recent raw journal state row presence")?;
    Ok(row.is_some())
}

fn upsert_recent_raw_journal_state_on_conn(
    conn: &Connection,
    state: &RecentRawJournalStateRow,
) -> Result<()> {
    conn.execute(
        "INSERT INTO recent_raw_journal_state(
            id,
            covered_since_ts,
            covered_through_cursor_ts,
            covered_through_cursor_slot,
            covered_through_cursor_signature,
            row_count,
            last_batch_rows,
            last_batch_completed_at,
            last_pruned_rows,
            last_pruned_at,
            updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(id) DO UPDATE SET
            covered_since_ts = excluded.covered_since_ts,
            covered_through_cursor_ts = excluded.covered_through_cursor_ts,
            covered_through_cursor_slot = excluded.covered_through_cursor_slot,
            covered_through_cursor_signature = excluded.covered_through_cursor_signature,
            row_count = excluded.row_count,
            last_batch_rows = excluded.last_batch_rows,
            last_batch_completed_at = excluded.last_batch_completed_at,
            last_pruned_rows = excluded.last_pruned_rows,
            last_pruned_at = excluded.last_pruned_at,
            updated_at = excluded.updated_at",
        params![
            1_i64,
            state.covered_since.map(|ts| ts.to_rfc3339()),
            state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.ts_utc.to_rfc3339()),
            state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.slot as i64),
            state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            state.row_count as i64,
            state.last_batch_rows as i64,
            state.last_batch_completed_at.map(|ts| ts.to_rfc3339()),
            state.last_pruned_rows as i64,
            state.last_pruned_at.map(|ts| ts.to_rfc3339()),
            state.updated_at.map(|ts| ts.to_rfc3339()),
        ],
    )
    .context("failed upserting recent raw journal state")?;
    Ok(())
}

impl SqliteStore {
    pub fn ensure_recent_raw_journal_tables(&self) -> Result<()> {
        ensure_recent_raw_journal_tables_on_conn(&self.conn)
    }

    pub fn recent_raw_journal_state(&self) -> Result<RecentRawJournalStateRow> {
        self.ensure_recent_raw_journal_tables()?;
        recent_raw_journal_state_query(&self.conn)
    }

    pub fn recent_raw_journal_state_read_only(&self) -> Result<RecentRawJournalStateRow> {
        if !self.sqlite_table_exists("observed_swaps")?
            || !self.sqlite_table_exists("recent_raw_journal_state")?
        {
            return Ok(RecentRawJournalStateRow::default());
        }
        recent_raw_journal_state_query(&self.conn)
    }

    pub fn recent_raw_journal_state_cached_read_only_required(
        &self,
    ) -> Result<RecentRawJournalStateRow> {
        if !self.sqlite_table_exists("recent_raw_journal_state")? {
            bail!("cached recent raw journal state table recent_raw_journal_state is missing");
        }
        if !recent_raw_journal_state_row_exists(&self.conn)? {
            bail!("cached recent raw journal state row id=1 is missing");
        }
        recent_raw_journal_state_cached_query(&self.conn)
    }

    pub fn recent_raw_journal_state_cached(&self) -> Result<RecentRawJournalStateRow> {
        self.ensure_recent_raw_journal_tables()?;
        recent_raw_journal_state_cached_query(&self.conn)
    }

    pub fn insert_recent_raw_journal_batch(
        &self,
        swaps: &[SwapEvent],
        completed_at: DateTime<Utc>,
    ) -> Result<RecentRawJournalWriteSummary> {
        let (summary, _time_budget_exhausted) =
            self.insert_recent_raw_journal_batch_internal(swaps, completed_at, None)?;
        Ok(summary)
    }

    pub fn insert_recent_raw_journal_batch_with_deadline(
        &self,
        swaps: &[SwapEvent],
        completed_at: DateTime<Utc>,
        deadline: Instant,
    ) -> Result<(RecentRawJournalWriteSummary, bool)> {
        self.insert_recent_raw_journal_batch_internal(swaps, completed_at, Some(deadline))
    }

    pub fn insert_recent_raw_journal_batch_bulk_with_deadline(
        &self,
        swaps: &[SwapEvent],
        completed_at: DateTime<Utc>,
        deadline: Instant,
    ) -> Result<(RecentRawJournalWriteSummary, bool)> {
        self.insert_recent_raw_journal_batch_bulk_with_deadline_internal(
            swaps,
            completed_at,
            deadline,
            None,
        )
    }

    fn insert_recent_raw_journal_batch_bulk_with_deadline_internal(
        &self,
        swaps: &[SwapEvent],
        completed_at: DateTime<Utc>,
        deadline: Instant,
        requested_chunk_rows: Option<usize>,
    ) -> Result<(RecentRawJournalWriteSummary, bool)> {
        self.ensure_recent_raw_journal_tables()?;
        if swaps.is_empty() {
            let state = self.recent_raw_journal_state_cached()?;
            return Ok((recent_raw_journal_write_summary(&state, 0, 0), false));
        }

        let transaction_started = Instant::now();
        let write_result = self.with_immediate_transaction_retry(
            "recent raw journal bulk batch write",
            |conn| {
                ensure_recent_raw_journal_tables_on_conn(conn)?;
                let sqlite_variable_limit = recent_raw_journal_sqlite_variable_limit(conn);
                let chunk_rows = recent_raw_journal_effective_bulk_insert_chunk_rows(
                    requested_chunk_rows,
                    sqlite_variable_limit,
                );
                let mut statement_count = 0usize;
                let mut inserted_rows = 0usize;
                let mut processed_rows = 0usize;
                let mut value_build_duration_ms = 0u64;
                let mut prepare_duration_ms = 0u64;
                let mut execute_duration_ms = 0u64;
                let mut state_refresh_duration_ms = 0u64;
                let mut state_upsert_duration_ms = 0u64;
                let mut deadline_exhausted_before_statement = false;
                let mut deadline_exhausted_during_execute = false;
                let mut time_budget_exhausted = false;

                for chunk in swaps.chunks(chunk_rows) {
                    if Instant::now() >= deadline {
                        deadline_exhausted_before_statement = true;
                        time_budget_exhausted = true;
                        break;
                    }

                    let bind_count =
                        chunk.len() * RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW;
                    if bind_count > sqlite_variable_limit {
                        bail!(
                            "recent raw journal bulk insert chunk would exceed SQLite variable limit: bind_count={bind_count} sqlite_variable_limit={sqlite_variable_limit}"
                        );
                    }

                    let sql = recent_raw_journal_bulk_insert_sql(chunk.len());
                    let value_build_started = Instant::now();
                    let mut values = Vec::with_capacity(
                        chunk
                            .len()
                            .saturating_mul(RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW),
                    );
                    for swap in chunk {
                        push_recent_raw_journal_bulk_insert_values(&mut values, swap);
                    }
                    value_build_duration_ms =
                        value_build_duration_ms.saturating_add(recent_raw_elapsed_ms(value_build_started));
                    let prepare_started = Instant::now();
                    let mut stmt = match conn.prepare_cached(&sql) {
                        Ok(stmt) => stmt,
                        Err(error)
                            if recent_raw_journal_sqlite_error_is_operation_interrupted(&error) =>
                        {
                            prepare_duration_ms =
                                prepare_duration_ms.saturating_add(recent_raw_elapsed_ms(prepare_started));
                            deadline_exhausted_before_statement = true;
                            time_budget_exhausted = true;
                            break;
                        }
                        Err(error) => {
                            return Err(error).context(
                                "failed to prepare recent raw journal bulk insert statement",
                            );
                        }
                    };
                    prepare_duration_ms =
                        prepare_duration_ms.saturating_add(recent_raw_elapsed_ms(prepare_started));
                    if Instant::now() >= deadline {
                        deadline_exhausted_before_statement = true;
                        time_budget_exhausted = true;
                        break;
                    }

                    statement_count = statement_count.saturating_add(1);
                    let execute_started = Instant::now();
                    let execute_result = {
                        let _progress_guard = ProgressHandlerGuard::install(conn, deadline);
                        stmt.execute(params_from_iter(values))
                    };
                    execute_duration_ms =
                        execute_duration_ms.saturating_add(recent_raw_elapsed_ms(execute_started));
                    let changed = match execute_result {
                        Ok(changed) => changed,
                        Err(error)
                            if recent_raw_journal_sqlite_error_is_operation_interrupted(&error) =>
                        {
                            deadline_exhausted_during_execute = true;
                            time_budget_exhausted = true;
                            break;
                        }
                        Err(error) => {
                            return Err(error).context(
                                "failed to bulk insert observed swaps into recent raw journal batch",
                            );
                        }
                    };
                    processed_rows = processed_rows.saturating_add(chunk.len());
                    inserted_rows = inserted_rows.saturating_add(changed);
                    if recent_raw_bulk_write_test_hook_requests_budget_exhaustion(
                        processed_rows,
                        inserted_rows,
                    ) {
                        time_budget_exhausted = true;
                        break;
                    }
                }

                let state_refresh_started = Instant::now();
                let mut state = recent_raw_journal_state_cached_query(conn)?;
                state_refresh_duration_ms = state_refresh_duration_ms
                    .saturating_add(recent_raw_elapsed_ms(state_refresh_started));
                advance_recent_raw_journal_state_for_batch(
                    &mut state,
                    &swaps[..processed_rows],
                    inserted_rows,
                    completed_at,
                );
                if processed_rows > 0 {
                    let state_upsert_started = Instant::now();
                    upsert_recent_raw_journal_state_on_conn(conn, &state)?;
                    state_upsert_duration_ms = state_upsert_duration_ms
                        .saturating_add(recent_raw_elapsed_ms(state_upsert_started));
                }
                let mut summary =
                    recent_raw_journal_write_summary(&state, processed_rows, inserted_rows);
                summary.recent_raw_bulk_sqlite_variable_limit = sqlite_variable_limit;
                summary.recent_raw_bulk_statement_params_per_row =
                    RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW;
                summary.recent_raw_bulk_statement_chunk_row_cap =
                    RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS;
                summary.recent_raw_bulk_effective_statement_chunk_rows = chunk_rows;
                summary.recent_raw_bulk_statement_count = statement_count;
                summary.recent_raw_bulk_rows_processed = processed_rows;
                summary.recent_raw_bulk_rows_inserted = inserted_rows;
                summary.recent_raw_bulk_value_build_duration_ms = value_build_duration_ms;
                summary.recent_raw_bulk_prepare_duration_ms = prepare_duration_ms;
                summary.recent_raw_bulk_execute_duration_ms = execute_duration_ms;
                summary.recent_raw_bulk_state_refresh_duration_ms = state_refresh_duration_ms;
                summary.recent_raw_bulk_state_upsert_duration_ms = state_upsert_duration_ms;
                summary.recent_raw_bulk_deadline_exhausted_before_statement =
                    deadline_exhausted_before_statement;
                summary.recent_raw_bulk_deadline_exhausted_during_execute =
                    deadline_exhausted_during_execute;
                Ok((
                    summary,
                    time_budget_exhausted,
                ))
            },
        );
        match write_result {
            Ok((mut summary, time_budget_exhausted)) => {
                summary.recent_raw_bulk_transaction_duration_ms =
                    recent_raw_elapsed_ms(transaction_started);
                Ok((summary, time_budget_exhausted))
            }
            Err(error) if recent_raw_journal_anyhow_error_is_operation_interrupted(&error) => {
                let state = self.recent_raw_journal_state_cached()?;
                Ok((recent_raw_journal_write_summary(&state, 0, 0), true))
            }
            Err(error) => Err(error),
        }
    }

    fn insert_recent_raw_journal_batch_internal(
        &self,
        swaps: &[SwapEvent],
        completed_at: DateTime<Utc>,
        deadline: Option<Instant>,
    ) -> Result<(RecentRawJournalWriteSummary, bool)> {
        self.ensure_recent_raw_journal_tables()?;
        if swaps.is_empty() {
            let state = self.recent_raw_journal_state_cached()?;
            return Ok((recent_raw_journal_write_summary(&state, 0, 0), false));
        }

        self.with_immediate_transaction_retry("recent raw journal batch write", |conn| {
            ensure_recent_raw_journal_tables_on_conn(conn)?;
            let mut inserted_rows = 0usize;
            let mut processed_rows = 0usize;
            let mut time_budget_exhausted = false;
            {
                let _progress_guard =
                    deadline.map(|deadline| ProgressHandlerGuard::install(conn, deadline));
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
                    .context("failed to prepare recent raw journal batch insert statement")?;

                for swap in swaps {
                    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        time_budget_exhausted = true;
                        break;
                    }
                    let changed = match stmt.execute(params![
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
                    ]) {
                        Ok(changed) => changed,
                        Err(error) => {
                            if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                                time_budget_exhausted = true;
                                break;
                            }
                            return Err(error).context(
                                "failed to insert observed swap into recent raw journal batch",
                            );
                        }
                    };
                    processed_rows = processed_rows.saturating_add(1);
                    if changed > 0 {
                        inserted_rows = inserted_rows.saturating_add(1);
                    }
                }
            }

            let mut state = recent_raw_journal_state_cached_query(conn)?;
            advance_recent_raw_journal_state_for_batch(
                &mut state,
                &swaps[..processed_rows],
                inserted_rows,
                completed_at,
            );
            if processed_rows > 0 {
                upsert_recent_raw_journal_state_on_conn(conn, &state)?;
            }
            Ok((
                recent_raw_journal_write_summary(&state, processed_rows, inserted_rows),
                time_budget_exhausted,
            ))
        })
    }

    pub fn prune_recent_raw_journal_before_batch(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
        pruned_at: DateTime<Utc>,
    ) -> Result<usize> {
        self.ensure_recent_raw_journal_tables()?;
        let cutoff_ts = cutoff.to_rfc3339();
        let batch_limit = batch_size.max(1).min(i64::MAX as usize) as i64;
        self.with_immediate_transaction_retry("recent raw journal retention prune", |conn| {
            ensure_recent_raw_journal_tables_on_conn(conn)?;
            let deleted = conn
                .execute(
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
                .context("failed deleting recent raw journal retention slice")?;
            let mut state = recent_raw_journal_state_query(conn)?;
            state.last_pruned_rows = deleted.max(0) as usize;
            state.last_pruned_at = Some(pruned_at);
            state.updated_at = Some(pruned_at);
            upsert_recent_raw_journal_state_on_conn(conn, &state)?;
            Ok(deleted.max(0) as usize)
        })
    }

    pub fn replay_recent_raw_journal_into_runtime_store(
        &self,
        runtime_store: &SqliteStore,
        required_window_start: DateTime<Utc>,
        artifact_runtime_cursor: &DiscoveryRuntimeCursor,
        replay_batch_size: usize,
    ) -> Result<RecentRawJournalReplaySummary> {
        let journal_state = self.recent_raw_journal_state_read_only()?;
        let journal_available = journal_state.row_count > 0;
        let journal_covers_artifact_cursor = journal_state
            .covered_through_cursor
            .as_ref()
            .is_some_and(|cursor| {
                discovery_runtime_cursor_cmp(cursor, artifact_runtime_cursor) != Ordering::Less
            });

        let mut replayed_rows = 0usize;
        if journal_available {
            let mut batch = Vec::with_capacity(replay_batch_size.max(1));
            self.for_each_observed_swap_since(required_window_start, |swap| {
                batch.push(swap);
                if batch.len() >= replay_batch_size.max(1) {
                    replayed_rows = replayed_rows.saturating_add(
                        runtime_store
                            .insert_observed_swaps_batch_with_activity_days(&batch)?
                            .into_iter()
                            .filter(|inserted| *inserted)
                            .count(),
                    );
                    batch.clear();
                }
                Ok(())
            })?;
            if !batch.is_empty() {
                replayed_rows = replayed_rows.saturating_add(
                    runtime_store
                        .insert_observed_swaps_batch_with_activity_days(&batch)?
                        .into_iter()
                        .filter(|inserted| *inserted)
                        .count(),
                );
            }
        }

        let runtime_window_has_rows = !runtime_store
            .load_recent_observed_swaps_since(required_window_start, 1)?
            .0
            .is_empty();
        let raw_coverage_satisfied = journal_available
            && journal_state
                .covered_since
                .is_some_and(|covered_since| covered_since <= required_window_start)
            && journal_covers_artifact_cursor
            && runtime_window_has_rows;

        Ok(RecentRawJournalReplaySummary {
            required_window_start,
            artifact_runtime_cursor: artifact_runtime_cursor.clone(),
            journal_available,
            journal_covered_since: journal_state.covered_since,
            journal_covered_through_cursor: journal_state.covered_through_cursor,
            journal_covers_artifact_cursor,
            replayed_rows,
            raw_coverage_satisfied,
        })
    }

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

    pub fn for_each_observed_swap_since_with_budget<F>(
        &self,
        since: DateTime<Utc>,
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
                 WHERE ts >= ?1
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?2",
            )
            .context("failed to prepare observed_swaps bounded streaming query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339(), limit])
            .context("failed to stream observed_swaps bounded rows")?;

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
                    return Err(error).context("failed iterating bounded observed_swaps stream");
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

        let mut total_rows_seen = 0usize;
        let mut time_budget_exhausted = false;
        let mut page_cursor = cursor.cloned();

        while total_rows_seen < limit && Instant::now() < deadline {
            let page_limit = limit
                .saturating_sub(total_rows_seen)
                .min(OBSERVED_SWAP_CURSOR_QUERY_PAGE_LIMIT);
            let mut next_cursor = page_cursor.clone();
            let page = self
                .for_each_observed_swap_in_window_after_cursor_single_statement_with_budget(
                    since,
                    until,
                    page_cursor.as_ref(),
                    page_limit,
                    deadline,
                    |swap| {
                        next_cursor = Some(DiscoveryRuntimeCursor {
                            ts_utc: swap.ts_utc,
                            slot: swap.slot,
                            signature: swap.signature.clone(),
                        });
                        on_swap(swap)
                    },
                )?;
            total_rows_seen = total_rows_seen.saturating_add(page.rows_seen);
            time_budget_exhausted |= page.time_budget_exhausted;
            if page.time_budget_exhausted || page.rows_seen < page_limit {
                break;
            }
            page_cursor = next_cursor;
        }

        if Instant::now() >= deadline && total_rows_seen < limit {
            time_budget_exhausted = true;
        }

        Ok(ObservedSwapCursorPage {
            rows_seen: total_rows_seen,
            time_budget_exhausted,
        })
    }

    fn for_each_observed_swap_in_window_after_cursor_single_statement_with_budget<F>(
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

    pub fn for_each_observed_swap_in_window_paged<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: usize,
        mut on_swap: F,
    ) -> Result<usize>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        Ok(self
            .for_each_observed_swap_in_window_paged_with_budget(
                since,
                until,
                limit,
                Instant::now() + StdDuration::from_secs(24 * 60 * 60),
                |swap| on_swap(swap),
            )?
            .rows_seen)
    }

    pub fn for_each_observed_swap_in_window_paged_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
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

        let mut total_rows_seen = 0usize;
        let mut time_budget_exhausted = false;
        let mut cursor: Option<DiscoveryRuntimeCursor> = None;

        loop {
            let page_cursor = cursor.clone();
            let mut next_cursor = page_cursor.clone();
            let page = self.for_each_observed_swap_in_window_after_cursor_with_budget(
                since,
                until,
                page_cursor.as_ref(),
                limit,
                deadline,
                |swap| {
                    next_cursor = Some(DiscoveryRuntimeCursor {
                        ts_utc: swap.ts_utc,
                        slot: swap.slot,
                        signature: swap.signature.clone(),
                    });
                    on_swap(swap)
                },
            )?;
            cursor = next_cursor;
            total_rows_seen = total_rows_seen.saturating_add(page.rows_seen);
            time_budget_exhausted |= page.time_budget_exhausted;
            if page.time_budget_exhausted || page.rows_seen < limit {
                break;
            }
        }

        Ok(ObservedSwapCursorPage {
            rows_seen: total_rows_seen,
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
        let wallet_ids_rows = wallet_ids_stmt
            .query(rusqlite::params_from_iter(wallet_ids_params))
            .context("failed querying observed wallet activity wallet-id page")?;
        let wallet_id_page =
            self.load_observed_wallet_activity_wallet_id_page_from_rows(wallet_ids_rows)?;
        if wallet_id_page.time_budget_exhausted {
            return Ok(ObservedWalletActivityPage {
                rows: Vec::new(),
                rows_seen: 0,
                time_budget_exhausted: true,
                active_day_count_source: None,
            });
        }
        self.observed_wallet_activity_page_for_wallet_ids_in_window_with_budget(
            &wallet_id_page.wallet_ids,
            since,
            until,
            max_tx_per_minute,
            deadline,
        )
    }

    pub fn observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
        &self,
        exact_wallet_ids: &[String],
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        wallet_cursor: Option<&str>,
        wallet_limit: usize,
        max_tx_per_minute: u32,
        deadline: Instant,
    ) -> Result<ObservedWalletActivityPage> {
        if exact_wallet_ids.is_empty() || wallet_limit == 0 {
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

        self.ensure_loaded_observed_wallet_activity_target_wallet_filter(exact_wallet_ids)?;
        let wallet_limit = wallet_limit.min(900).max(1) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let (wallet_ids_query, wallet_ids_params): (&str, Vec<rusqlite::types::Value>) =
            match wallet_cursor {
                Some(wallet_cursor) => (
                    "SELECT wallet_id
                     FROM temp_discovery_replay_candidate_wallets
                     WHERE wallet_id > ?1
                     ORDER BY wallet_id ASC
                     LIMIT ?2",
                    vec![wallet_cursor.to_string().into(), wallet_limit.into()],
                ),
                None => (
                    "SELECT wallet_id
                     FROM temp_discovery_replay_candidate_wallets
                     ORDER BY wallet_id ASC
                     LIMIT ?1",
                    vec![wallet_limit.into()],
                ),
            };
        let mut wallet_ids_stmt = self
            .conn
            .prepare(wallet_ids_query)
            .context("failed to prepare exact observed wallet activity wallet-id page query")?;
        let wallet_ids_rows = wallet_ids_stmt
            .query(rusqlite::params_from_iter(wallet_ids_params))
            .context("failed querying exact observed wallet activity wallet-id page")?;
        let wallet_id_page =
            self.load_observed_wallet_activity_wallet_id_page_from_rows(wallet_ids_rows)?;
        if wallet_id_page.time_budget_exhausted {
            return Ok(ObservedWalletActivityPage {
                rows: Vec::new(),
                rows_seen: 0,
                time_budget_exhausted: true,
                active_day_count_source: None,
            });
        }
        self.observed_wallet_activity_page_for_wallet_ids_in_window_with_budget(
            &wallet_id_page.wallet_ids,
            since,
            until,
            max_tx_per_minute,
            deadline,
        )
    }

    fn load_observed_wallet_activity_wallet_id_page_from_rows(
        &self,
        mut wallet_ids_rows: rusqlite::Rows<'_>,
    ) -> Result<ObservedWalletActivityWalletIdPage> {
        let mut wallet_id_page = ObservedWalletActivityWalletIdPage::default();
        loop {
            let next_row = match wallet_ids_rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        wallet_id_page.time_budget_exhausted = true;
                        return Ok(wallet_id_page);
                    }
                    return Err(error)
                        .context("failed iterating observed wallet activity wallet-id rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            wallet_id_page.wallet_ids.push(
                row.get::<_, String>(0)
                    .context("failed reading observed wallet activity wallet_id")?,
            );
        }
        Ok(wallet_id_page)
    }

    fn observed_wallet_activity_page_for_wallet_ids_in_window_with_budget(
        &self,
        wallet_ids: &[String],
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        max_tx_per_minute: u32,
        deadline: Instant,
    ) -> Result<ObservedWalletActivityPage> {
        if wallet_ids.is_empty() {
            return Ok(ObservedWalletActivityPage::default());
        }

        let placeholders = std::iter::repeat_n("?", wallet_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let since_raw = since.to_rfc3339();
        let until_raw = until.to_rfc3339();

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

        let active_day_summaries = self
            .observed_wallet_activity_day_summaries_in_window_with_budget(
                wallet_ids, since, until, deadline,
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
                    wallet_ids, since, until, deadline,
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
            for wallet_id in wallet_ids {
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
        for wallet_id in wallet_ids {
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
            .iter()
            .filter_map(|wallet_id| summaries.remove(wallet_id))
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
        let access_path = self.observed_sol_leg_cursor_access_path()?;
        if limit == 0 {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: false,
                access_path,
            });
        }
        if Instant::now() >= deadline {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: true,
                access_path,
            });
        }

        let mut total_rows_seen = 0usize;
        let mut time_budget_exhausted = false;
        let mut page_cursor = cursor.cloned();

        while total_rows_seen < limit && Instant::now() < deadline {
            let page_limit = limit
                .saturating_sub(total_rows_seen)
                .min(OBSERVED_SOL_LEG_CURSOR_QUERY_PAGE_LIMIT);
            let mut next_cursor = page_cursor.clone();
            let page = self
                .for_each_observed_sol_leg_swap_in_window_after_cursor_single_statement_with_budget(
                    since,
                    until,
                    page_cursor.as_ref(),
                    page_limit,
                    deadline,
                    |swap| {
                        next_cursor = Some(DiscoveryRuntimeCursor {
                            ts_utc: swap.ts_utc,
                            slot: swap.slot,
                            signature: swap.signature.clone(),
                        });
                        on_swap(swap)
                    },
                )?;
            total_rows_seen = total_rows_seen.saturating_add(page.rows_seen);
            time_budget_exhausted |= page.time_budget_exhausted;
            if page.time_budget_exhausted || page.rows_seen < page_limit {
                return Ok(ObservedSolLegCursorPage {
                    rows_seen: total_rows_seen,
                    time_budget_exhausted,
                    access_path,
                });
            }
            page_cursor = next_cursor;
        }

        if Instant::now() >= deadline && total_rows_seen < limit {
            time_budget_exhausted = true;
        }

        Ok(ObservedSolLegCursorPage {
            rows_seen: total_rows_seen,
            time_budget_exhausted,
            access_path,
        })
    }

    pub fn for_each_observed_sol_leg_swap_in_window_after_cursor_for_target_buy_mints_with_budget<
        F,
    >(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
        target_buy_mints: &[String],
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSolLegCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        if target_buy_mints.is_empty() {
            return self.for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget(
                since, until, cursor, limit, deadline, on_swap,
            );
        }

        self.ensure_loaded_observed_sol_leg_target_buy_mint_filter(target_buy_mints)?;
        let access_path = self.observed_sol_leg_cursor_access_path()?;
        if limit == 0 {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: false,
                access_path,
            });
        }
        if Instant::now() >= deadline {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: true,
                access_path,
            });
        }

        let mut total_rows_seen = 0usize;
        let mut time_budget_exhausted = false;
        let mut page_cursor = cursor.cloned();

        while total_rows_seen < limit && Instant::now() < deadline {
            let page_limit = limit
                .saturating_sub(total_rows_seen)
                .min(OBSERVED_SOL_LEG_CURSOR_QUERY_PAGE_LIMIT);
            let mut next_cursor = page_cursor.clone();
            let page = self
                .for_each_observed_sol_leg_swap_in_window_after_cursor_single_statement_for_loaded_target_buy_mint_filter_with_budget(
                    since,
                    until,
                    page_cursor.as_ref(),
                    page_limit,
                    deadline,
                    |swap| {
                        next_cursor = Some(DiscoveryRuntimeCursor {
                            ts_utc: swap.ts_utc,
                            slot: swap.slot,
                            signature: swap.signature.clone(),
                        });
                        on_swap(swap)
                    },
                )?;
            total_rows_seen = total_rows_seen.saturating_add(page.rows_seen);
            time_budget_exhausted |= page.time_budget_exhausted;
            if page.time_budget_exhausted || page.rows_seen < page_limit {
                return Ok(ObservedSolLegCursorPage {
                    rows_seen: total_rows_seen,
                    time_budget_exhausted,
                    access_path,
                });
            }
            page_cursor = next_cursor;
        }

        if Instant::now() >= deadline && total_rows_seen < limit {
            time_budget_exhausted = true;
        }

        Ok(ObservedSolLegCursorPage {
            rows_seen: total_rows_seen,
            time_budget_exhausted,
            access_path,
        })
    }

    fn observed_sol_leg_cursor_access_path(&self) -> Result<ObservedSolLegCursorAccessPath> {
        Ok(
            if self.sqlite_index_exists("idx_observed_swaps_sol_leg_ts_slot_signature")? {
                ObservedSolLegCursorAccessPath::SolLegPartialIndex
            } else {
                ObservedSolLegCursorAccessPath::TsCursorFallback
            },
        )
    }

    fn ensure_observed_sol_leg_target_buy_mint_filter_table(&self) -> Result<()> {
        self.conn
            .execute_batch(&format!(
                "CREATE TEMP TABLE IF NOT EXISTS {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE} (
                    mint TEXT PRIMARY KEY
                ) WITHOUT ROWID;"
            ))
            .context("failed ensuring temporary observed SOL-leg target-mint filter table exists")
    }

    fn ensure_observed_sol_leg_target_buy_mint_filter_meta_table(&self) -> Result<()> {
        self.conn
            .execute_batch(&format!(
                "CREATE TEMP TABLE IF NOT EXISTS {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_META_TABLE} (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    fingerprint TEXT NOT NULL,
                    mint_count INTEGER NOT NULL
                ) WITHOUT ROWID;"
            ))
            .context(
                "failed ensuring temporary observed SOL-leg target-mint filter metadata table exists",
            )
    }

    fn observed_sol_leg_target_buy_mint_filter_fingerprint(target_buy_mints: &[String]) -> String {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        target_buy_mints.len().hash(&mut hasher);
        for mint in target_buy_mints {
            mint.hash(&mut hasher);
        }
        format!("{:016x}", hasher.finish())
    }

    fn ensure_loaded_observed_sol_leg_target_buy_mint_filter(
        &self,
        target_buy_mints: &[String],
    ) -> Result<()> {
        self.ensure_observed_sol_leg_target_buy_mint_filter_table()?;
        self.ensure_observed_sol_leg_target_buy_mint_filter_meta_table()?;

        let fingerprint =
            Self::observed_sol_leg_target_buy_mint_filter_fingerprint(target_buy_mints);
        let expected_count = target_buy_mints.len() as i64;
        let loaded = self
            .conn
            .query_row(
                &format!(
                    "SELECT fingerprint, mint_count
                     FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_META_TABLE}
                     WHERE id = 1"
                ),
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .context("failed reading temporary observed SOL-leg target-mint filter metadata row")?;
        let loaded_count = self
            .conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}"),
                [],
                |row| row.get::<_, i64>(0),
            )
            .context("failed counting temporary observed SOL-leg target-mint filter rows")?;
        if loaded
            .as_ref()
            .is_some_and(|(loaded_fingerprint, loaded_mint_count)| {
                loaded_fingerprint == &fingerprint
                    && *loaded_mint_count == expected_count
                    && loaded_count == expected_count
            })
        {
            return Ok(());
        }

        self.replace_observed_sol_leg_target_buy_mint_filter(target_buy_mints)?;
        self.conn
            .execute(
                &format!(
                    "INSERT INTO {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_META_TABLE}(
                        id,
                        fingerprint,
                        mint_count
                    ) VALUES (1, ?1, ?2)
                    ON CONFLICT(id) DO UPDATE SET
                        fingerprint = excluded.fingerprint,
                        mint_count = excluded.mint_count"
                ),
                params![fingerprint, expected_count],
            )
            .context(
                "failed persisting temporary observed SOL-leg target-mint filter metadata row",
            )?;
        Ok(())
    }

    fn replace_observed_sol_leg_target_buy_mint_filter(
        &self,
        target_buy_mints: &[String],
    ) -> Result<()> {
        self.ensure_observed_sol_leg_target_buy_mint_filter_table()?;
        self.conn
            .execute(
                &format!("DELETE FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}"),
                [],
            )
            .context("failed clearing temporary observed SOL-leg target-mint filter table")?;
        let mut insert = self
            .conn
            .prepare_cached(&format!(
                "INSERT OR IGNORE INTO {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}(mint)
                 VALUES (?1)"
            ))
            .context("failed preparing temporary observed SOL-leg target-mint filter insert")?;
        for mint in target_buy_mints {
            insert
                .execute(params![mint])
                .with_context(|| format!("failed inserting target buy mint into temporary observed SOL-leg filter table: {mint}"))?;
        }
        Ok(())
    }

    fn ensure_observed_wallet_activity_target_wallet_filter_table(&self) -> Result<()> {
        self.conn
            .execute_batch(&format!(
                "CREATE TEMP TABLE IF NOT EXISTS {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_TABLE} (
                    wallet_id TEXT PRIMARY KEY
                ) WITHOUT ROWID;"
            ))
            .context(
                "failed ensuring temporary observed wallet activity target-wallet filter table exists",
            )
    }

    fn ensure_observed_wallet_activity_target_wallet_filter_meta_table(&self) -> Result<()> {
        self.conn
            .execute_batch(&format!(
                "CREATE TEMP TABLE IF NOT EXISTS {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_META_TABLE} (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    fingerprint TEXT NOT NULL,
                    wallet_count INTEGER NOT NULL
                ) WITHOUT ROWID;"
            ))
            .context(
                "failed ensuring temporary observed wallet activity target-wallet filter metadata table exists",
            )
    }

    fn canonical_observed_wallet_activity_target_wallet_ids(wallet_ids: &[String]) -> Vec<String> {
        let mut canonical = wallet_ids.to_vec();
        canonical.sort();
        canonical.dedup();
        canonical
    }

    fn observed_wallet_activity_target_wallet_filter_fingerprint(wallet_ids: &[String]) -> String {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        wallet_ids.len().hash(&mut hasher);
        for wallet_id in wallet_ids {
            wallet_id.hash(&mut hasher);
        }
        format!("{:016x}", hasher.finish())
    }

    fn ensure_loaded_observed_wallet_activity_target_wallet_filter(
        &self,
        wallet_ids: &[String],
    ) -> Result<()> {
        self.ensure_observed_wallet_activity_target_wallet_filter_table()?;
        self.ensure_observed_wallet_activity_target_wallet_filter_meta_table()?;

        let canonical_wallet_ids =
            Self::canonical_observed_wallet_activity_target_wallet_ids(wallet_ids);
        let fingerprint =
            Self::observed_wallet_activity_target_wallet_filter_fingerprint(&canonical_wallet_ids);
        let expected_count = canonical_wallet_ids.len() as i64;
        let loaded = self
            .conn
            .query_row(
                &format!(
                    "SELECT fingerprint, wallet_count
                     FROM {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_META_TABLE}
                     WHERE id = 1"
                ),
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .context(
                "failed reading temporary observed wallet activity target-wallet metadata row",
            )?;
        let loaded_count = self
            .conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_TABLE}"
                ),
                [],
                |row| row.get::<_, i64>(0),
            )
            .context("failed counting temporary observed wallet activity target-wallet rows")?;
        if loaded
            .as_ref()
            .is_some_and(|(loaded_fingerprint, loaded_wallet_count)| {
                loaded_fingerprint == &fingerprint
                    && *loaded_wallet_count == expected_count
                    && loaded_count == expected_count
            })
        {
            return Ok(());
        }

        self.replace_observed_wallet_activity_target_wallet_filter(&canonical_wallet_ids)?;
        self.conn
            .execute(
                &format!(
                    "INSERT INTO {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_META_TABLE}(
                        id,
                        fingerprint,
                        wallet_count
                    ) VALUES (1, ?1, ?2)
                    ON CONFLICT(id) DO UPDATE SET
                        fingerprint = excluded.fingerprint,
                        wallet_count = excluded.wallet_count"
                ),
                params![fingerprint, expected_count],
            )
            .context(
                "failed persisting temporary observed wallet activity target-wallet metadata row",
            )?;
        Ok(())
    }

    fn replace_observed_wallet_activity_target_wallet_filter(
        &self,
        wallet_ids: &[String],
    ) -> Result<()> {
        self.ensure_observed_wallet_activity_target_wallet_filter_table()?;
        self.conn
            .execute(
                &format!("DELETE FROM {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_TABLE}"),
                [],
            )
            .context("failed clearing temporary observed wallet activity target-wallet table")?;
        let mut insert = self
            .conn
            .prepare_cached(&format!(
                "INSERT OR IGNORE INTO {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_TABLE}(wallet_id)
                 VALUES (?1)"
            ))
            .context("failed preparing temporary observed wallet activity target-wallet insert")?;
        for wallet_id in wallet_ids {
            insert.execute(params![wallet_id]).with_context(|| {
                format!(
                    "failed inserting wallet into temporary observed wallet activity filter table: {wallet_id}"
                )
            })?;
        }
        Ok(())
    }

    fn for_each_observed_sol_leg_swap_in_window_after_cursor_single_statement_with_budget<F>(
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
        let access_path = self.observed_sol_leg_cursor_access_path()?;
        if limit == 0 {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: false,
                access_path,
            });
        }
        if Instant::now() >= deadline {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: true,
                access_path,
            });
        }

        let index_hint = match access_path {
            ObservedSolLegCursorAccessPath::SolLegPartialIndex => {
                "INDEXED BY idx_observed_swaps_sol_leg_ts_slot_signature"
            }
            ObservedSolLegCursorAccessPath::TsCursorFallback => {
                "INDEXED BY idx_observed_swaps_ts_slot_signature"
            }
        };
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

    fn for_each_observed_sol_leg_swap_in_window_after_cursor_single_statement_for_loaded_target_buy_mint_filter_with_budget<
        F,
    >(
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
        let access_path = self.observed_sol_leg_cursor_access_path()?;
        if limit == 0 {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: false,
                access_path,
            });
        }
        if Instant::now() >= deadline {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: true,
                access_path,
            });
        }

        let index_hint = match access_path {
            ObservedSolLegCursorAccessPath::SolLegPartialIndex => {
                "INDEXED BY idx_observed_swaps_sol_leg_ts_slot_signature"
            }
            ObservedSolLegCursorAccessPath::TsCursorFallback => {
                "INDEXED BY idx_observed_swaps_ts_slot_signature"
            }
        };
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
                       AND (
                            (token_in = '{SOL_MINT}'
                                AND token_out IN (
                                    SELECT mint FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}
                                ))
                         OR (token_out = '{SOL_MINT}'
                                AND token_in IN (
                                    SELECT mint FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}
                                ))
                       )
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
                       AND (
                            (token_in = '{SOL_MINT}'
                                AND token_out IN (
                                    SELECT mint FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}
                                ))
                         OR (token_out = '{SOL_MINT}'
                                AND token_in IN (
                                    SELECT mint FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}
                                ))
                       )
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
            .context("failed to prepare observed_swaps exact-target SOL-leg window cursor query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed to query observed_swaps exact-target SOL-leg window cursor")?;

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
                    return Err(error).context(
                        "failed iterating observed_swaps exact-target SOL-leg window cursor rows",
                    );
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
        let mut total_rows_seen = 0usize;
        let mut time_budget_exhausted = false;
        let mut page_cursor = DiscoveryRuntimeCursor {
            ts_utc: cursor_ts,
            slot: cursor_slot,
            signature: cursor_signature.to_string(),
        };

        while total_rows_seen < limit && Instant::now() < deadline {
            let page_limit = limit
                .saturating_sub(total_rows_seen)
                .min(OBSERVED_SWAP_CURSOR_QUERY_PAGE_LIMIT);
            let mut next_cursor = page_cursor.clone();
            let page = self.for_each_observed_swap_after_cursor_single_statement_with_budget(
                page_cursor.ts_utc,
                page_cursor.slot,
                page_cursor.signature.as_str(),
                page_limit,
                deadline,
                |swap| {
                    next_cursor = DiscoveryRuntimeCursor {
                        ts_utc: swap.ts_utc,
                        slot: swap.slot,
                        signature: swap.signature.clone(),
                    };
                    on_swap(swap)
                },
            )?;
            total_rows_seen = total_rows_seen.saturating_add(page.rows_seen);
            time_budget_exhausted |= page.time_budget_exhausted;
            if page.time_budget_exhausted || page.rows_seen < page_limit {
                break;
            }
            page_cursor = next_cursor;
        }

        if Instant::now() >= deadline && total_rows_seen < limit {
            time_budget_exhausted = true;
        }

        Ok(ObservedSwapCursorPage {
            rows_seen: total_rows_seen,
            time_budget_exhausted,
        })
    }

    fn for_each_observed_swap_after_cursor_single_statement_with_budget<F>(
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

    pub fn observed_swaps_coverage_snapshot(&self) -> Result<ObservedSwapsCoverageSnapshot> {
        let row_count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM observed_swaps", [], |row| row.get(0))
            .context("failed counting observed_swaps rows")?;
        let covered_since_raw: Option<String> = self
            .conn
            .query_row("SELECT MIN(ts) FROM observed_swaps", [], |row| row.get(0))
            .optional()
            .context("failed loading observed_swaps covered_since timestamp")?
            .flatten();
        let covered_since = parse_optional_rfc3339_utc(covered_since_raw, "observed_swaps.ts")?;
        let covered_through_cursor_raw = self
            .conn
            .query_row(
                "SELECT ts, slot, signature
                 FROM observed_swaps
                 ORDER BY ts DESC, slot DESC, signature DESC
                 LIMIT 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get(2)?)),
            )
            .optional()
            .context("failed loading observed_swaps covered_through cursor")?;
        let covered_through_cursor = covered_through_cursor_raw
            .map(
                |(ts_raw, slot_raw, signature)| -> Result<DiscoveryRuntimeCursor> {
                    Ok(DiscoveryRuntimeCursor {
                        ts_utc: parse_rfc3339_utc(&ts_raw, "observed_swaps.ts")?,
                        slot: slot_raw.max(0) as u64,
                        signature,
                    })
                },
            )
            .transpose()?;
        Ok(ObservedSwapsCoverageSnapshot {
            covered_since,
            covered_through_cursor,
            row_count: row_count.max(0) as usize,
        })
    }

    pub fn recent_observed_swap_counts_for_wallets(
        &self,
        since: DateTime<Utc>,
        wallet_ids: &[String],
    ) -> Result<Vec<WalletRecentActivityCountRow>> {
        if wallet_ids.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = std::iter::repeat_n("?", wallet_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT wallet_id, COUNT(*), MAX(ts)
             FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
             WHERE ts >= ?1
               AND wallet_id IN ({placeholders})
             GROUP BY wallet_id
             ORDER BY wallet_id ASC"
        );
        let mut params = vec![rusqlite::types::Value::from(since.to_rfc3339())];
        params.extend(wallet_ids.iter().cloned().map(rusqlite::types::Value::from));
        let mut stmt = self
            .conn
            .prepare(&query)
            .context("failed to prepare recent observed_swaps wallet activity query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed querying recent observed_swaps wallet activity")?;

        let mut summaries = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating recent observed_swaps wallet activity rows")?
        {
            let wallet_id: String = row
                .get(0)
                .context("failed reading recent observed_swaps wallet_id")?;
            let row_count_raw: i64 = row
                .get(1)
                .context("failed reading recent observed_swaps row_count")?;
            let latest_ts_raw: String = row
                .get(2)
                .context("failed reading recent observed_swaps latest_ts")?;
            summaries.push(WalletRecentActivityCountRow {
                wallet_id,
                row_count: row_count_raw.max(0) as usize,
                latest_ts: parse_rfc3339_utc(&latest_ts_raw, "recent observed_swaps latest_ts")?,
            });
        }
        Ok(summaries)
    }

    pub fn load_discovery_runtime_cursor(&self) -> Result<Option<DiscoveryRuntimeCursor>> {
        self.ensure_discovery_runtime_state_table()?;
        self.load_discovery_runtime_cursor_query()
    }

    pub fn load_discovery_runtime_cursor_read_only(
        &self,
    ) -> Result<Option<DiscoveryRuntimeCursor>> {
        if !self.sqlite_table_exists("discovery_runtime_state")? {
            return Ok(None);
        }
        self.load_discovery_runtime_cursor_query()
    }

    fn load_discovery_runtime_cursor_query(&self) -> Result<Option<DiscoveryRuntimeCursor>> {
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
        self.load_discovery_persisted_rebuild_state_query()
    }

    pub fn load_discovery_persisted_rebuild_state_read_only(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateRow>> {
        if !self.sqlite_table_exists("discovery_persisted_rebuild_state")? {
            return Ok(None);
        }
        self.load_discovery_persisted_rebuild_state_query()
    }

    pub fn discovery_persisted_rebuild_state_table_exists_read_only(&self) -> Result<bool> {
        self.sqlite_table_exists("discovery_persisted_rebuild_state")
    }

    pub fn load_discovery_persisted_rebuild_state_meta_lite_raw_after_table_exists_read_only(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateMetaLiteRawRow>> {
        let raw = self
            .conn
            .query_row(
                "SELECT
                    phase,
                    updated_at
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .context("failed reading discovery persisted rebuild state lite metadata")?;

        Ok(raw.map(
            |(phase_raw, updated_at_raw)| DiscoveryPersistedRebuildStateMetaLiteRawRow {
                phase_raw,
                updated_at_raw,
            },
        ))
    }

    pub fn explain_discovery_persisted_rebuild_state_meta_query_plan_after_table_exists_read_only(
        &self,
    ) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT phase, updated_at
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
            )
            .context("failed preparing discovery persisted rebuild state meta query plan")?;
        let plan_rows = stmt
            .query_map([], |row| row.get::<_, String>(3))
            .context("failed querying discovery persisted rebuild state meta query plan")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed collecting discovery persisted rebuild state meta query plan")?;
        Ok(plan_rows)
    }

    pub fn load_discovery_persisted_rebuild_state_meta_lite_raw_read_only(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateMetaLiteRawRow>> {
        if !self.sqlite_table_exists("discovery_persisted_rebuild_state")? {
            return Ok(None);
        }
        self.load_discovery_persisted_rebuild_state_meta_lite_raw_after_table_exists_read_only()
    }

    pub fn load_discovery_persisted_rebuild_state_json_bytes_after_table_exists_read_only(
        &self,
    ) -> Result<Option<usize>> {
        let state_json_bytes = self
            .conn
            .query_row(
                "SELECT length(CAST(state_json AS BLOB))
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .context("failed reading discovery persisted rebuild state json byte length")?;
        Ok(state_json_bytes.map(|bytes| bytes.max(0) as usize))
    }

    pub fn explain_discovery_persisted_rebuild_state_size_query_plan_after_table_exists_read_only(
        &self,
    ) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT length(CAST(state_json AS BLOB))
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
            )
            .context("failed preparing discovery persisted rebuild state size query plan")?;
        let plan_rows = stmt
            .query_map([], |row| row.get::<_, String>(3))
            .context("failed querying discovery persisted rebuild state size query plan")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed collecting discovery persisted rebuild state size query plan")?;
        Ok(plan_rows)
    }

    pub fn probe_discovery_persisted_rebuild_row_driver_compare_read_only(
        runtime_db_path: &Path,
        options: &DiscoveryPersistedRebuildRowDriverCompareOptions,
    ) -> Result<DiscoveryPersistedRebuildRowDriverCompareDiagnostic> {
        #[derive(Debug, Clone)]
        struct ProgressSnapshot {
            open_db_elapsed_ms: Option<u64>,
            load_connection_facts_elapsed_ms: Option<u64>,
            prepare_exists_elapsed_ms: Option<u64>,
            step_exists_elapsed_ms: Option<u64>,
            prepare_meta_elapsed_ms: Option<u64>,
            step_meta_elapsed_ms: Option<u64>,
            extract_phase_elapsed_ms: Option<u64>,
            extract_updated_at_elapsed_ms: Option<u64>,
            prepare_size_elapsed_ms: Option<u64>,
            step_size_elapsed_ms: Option<u64>,
            row_exists: Option<bool>,
            row_phase: Option<String>,
            row_updated_at: Option<String>,
            row_state_json_bytes: Option<usize>,
            connection_facts: Option<crate::SqliteReadOnlyDriverCompareFacts>,
        }

        impl ProgressSnapshot {
            fn new() -> Self {
                Self {
                    open_db_elapsed_ms: None,
                    load_connection_facts_elapsed_ms: None,
                    prepare_exists_elapsed_ms: None,
                    step_exists_elapsed_ms: None,
                    prepare_meta_elapsed_ms: None,
                    step_meta_elapsed_ms: None,
                    extract_phase_elapsed_ms: None,
                    extract_updated_at_elapsed_ms: None,
                    prepare_size_elapsed_ms: None,
                    step_size_elapsed_ms: None,
                    row_exists: None,
                    row_phase: None,
                    row_updated_at: None,
                    row_state_json_bytes: None,
                    connection_facts: None,
                }
            }
        }

        #[derive(Debug)]
        enum WorkerMessage {
            Entered(DiscoveryPersistedRebuildRowDriverCompareStage),
            Snapshot(ProgressSnapshot),
            Finished(Result<(), String>),
        }

        fn skipped_stages(
            current_stage: DiscoveryPersistedRebuildRowDriverCompareStage,
            budget_exhausted: bool,
        ) -> Vec<DiscoveryPersistedRebuildRowDriverCompareStage> {
            if !budget_exhausted {
                return Vec::new();
            }
            let relevant = [
                DiscoveryPersistedRebuildRowDriverCompareStage::OpenDbReadOnly,
                DiscoveryPersistedRebuildRowDriverCompareStage::LoadConnectionFacts,
                DiscoveryPersistedRebuildRowDriverCompareStage::PrepareExists,
                DiscoveryPersistedRebuildRowDriverCompareStage::StepExists,
                DiscoveryPersistedRebuildRowDriverCompareStage::PrepareMeta,
                DiscoveryPersistedRebuildRowDriverCompareStage::StepMeta,
                DiscoveryPersistedRebuildRowDriverCompareStage::ExtractPhase,
                DiscoveryPersistedRebuildRowDriverCompareStage::ExtractUpdatedAt,
                DiscoveryPersistedRebuildRowDriverCompareStage::PrepareSize,
                DiscoveryPersistedRebuildRowDriverCompareStage::StepSize,
                DiscoveryPersistedRebuildRowDriverCompareStage::Complete,
            ];
            let cutoff_index = relevant
                .iter()
                .position(|stage| *stage == current_stage)
                .unwrap_or(relevant.len());
            relevant
                .into_iter()
                .skip(cutoff_index.saturating_add(1))
                .collect()
        }

        let started_at = Instant::now();
        let deadline = started_at + StdDuration::from_millis(options.budget_ms);
        let mut diagnostic = DiscoveryPersistedRebuildRowDriverCompareDiagnostic {
            stage: DiscoveryPersistedRebuildRowDriverCompareStage::OpenDbReadOnly,
            budget_exhausted: false,
            skipped_stages: Vec::new(),
            open_db_elapsed_ms: None,
            load_connection_facts_elapsed_ms: None,
            prepare_exists_elapsed_ms: None,
            step_exists_elapsed_ms: None,
            prepare_meta_elapsed_ms: None,
            step_meta_elapsed_ms: None,
            extract_phase_elapsed_ms: None,
            extract_updated_at_elapsed_ms: None,
            prepare_size_elapsed_ms: None,
            step_size_elapsed_ms: None,
            total_elapsed_ms: 0,
            row_exists: None,
            row_phase: None,
            row_updated_at: None,
            row_state_json_bytes: None,
            connection_facts: None,
        };
        let (tx, rx) = mpsc::sync_channel(16);
        let runtime_db_path = runtime_db_path.to_path_buf();
        let options = options.clone();
        std::thread::spawn(move || {
            let send_finished = |result: Result<(), anyhow::Error>| {
                let _ = tx.send(WorkerMessage::Finished(
                    result.map_err(|error| format!("{error:#}")),
                ));
            };
            let mut snapshot = ProgressSnapshot::new();

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowDriverCompareStage::OpenDbReadOnly,
            ));
            let open_started_at = Instant::now();
            let store = match SqliteStore::open_read_only(&runtime_db_path).with_context(|| {
                format!(
                    "failed opening runtime sqlite db read-only {}",
                    runtime_db_path.display()
                )
            }) {
                Ok(store) => store,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.open_db_elapsed_ms =
                Some(open_started_at.elapsed().as_millis().min(u64::MAX as u128) as u64);
            if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                return;
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowDriverCompareStage::LoadConnectionFacts,
            ));
            let facts_started_at = Instant::now();
            let facts = match store.sqlite_read_only_driver_compare_facts() {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.load_connection_facts_elapsed_ms =
                Some(facts_started_at.elapsed().as_millis().min(u64::MAX as u128) as u64);
            snapshot.connection_facts = Some(facts);
            if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                return;
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowDriverCompareStage::PrepareExists,
            ));
            let prepare_exists_started_at = Instant::now();
            if let Some(delay_ms) = options.test_force_prepare_exists_delay_ms {
                std::thread::sleep(StdDuration::from_millis(delay_ms));
            }
            let mut exists_stmt = match store
                .conn
                .prepare("SELECT 1 FROM discovery_persisted_rebuild_state WHERE id = 1")
                .context("failed preparing persisted rebuild exists probe")
            {
                Ok(stmt) => stmt,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.prepare_exists_elapsed_ms = Some(
                prepare_exists_started_at
                    .elapsed()
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
            );
            if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                return;
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowDriverCompareStage::StepExists,
            ));
            let step_exists_started_at = Instant::now();
            if let Some(delay_ms) = options.test_force_step_exists_delay_ms {
                std::thread::sleep(StdDuration::from_millis(delay_ms));
            }
            let exists_row = {
                let mut exists_rows = match exists_stmt.query([]) {
                    Ok(rows) => rows,
                    Err(error) => {
                        send_finished(
                            Err(error).context("failed querying persisted rebuild exists probe"),
                        );
                        return;
                    }
                };
                match exists_rows.next() {
                    Ok(value) => value.is_some(),
                    Err(error) => {
                        send_finished(
                            Err(error).context("failed stepping persisted rebuild exists probe"),
                        );
                        return;
                    }
                }
            };
            snapshot.step_exists_elapsed_ms = Some(
                step_exists_started_at
                    .elapsed()
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
            );
            snapshot.row_exists = Some(exists_row);
            if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                return;
            }
            drop(exists_stmt);

            if !exists_row {
                let _ = tx.send(WorkerMessage::Finished(Ok(())));
                return;
            }

            if options.test_require_no_active_statements_before_prepare_meta {
                let active_statement_count = store.sqlite_active_statement_count_for_debug();
                if active_statement_count != 0 {
                    send_finished(Err(anyhow!(
                        "expected no active statements before prepare_meta, found {active_statement_count}"
                    )));
                    return;
                }
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowDriverCompareStage::PrepareMeta,
            ));
            let prepare_meta_started_at = Instant::now();
            if let Some(delay_ms) = options.test_force_prepare_meta_delay_ms {
                std::thread::sleep(StdDuration::from_millis(delay_ms));
            }
            let mut meta_stmt = match store
                .conn
                .prepare(
                    "SELECT phase, updated_at
                     FROM discovery_persisted_rebuild_state
                     WHERE id = 1",
                )
                .context("failed preparing persisted rebuild row meta query")
            {
                Ok(stmt) => stmt,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.prepare_meta_elapsed_ms = Some(
                prepare_meta_started_at
                    .elapsed()
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
            );
            if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                return;
            }

            {
                let _ = tx.send(WorkerMessage::Entered(
                    DiscoveryPersistedRebuildRowDriverCompareStage::StepMeta,
                ));
                let step_meta_started_at = Instant::now();
                if let Some(delay_ms) = options.test_force_step_meta_delay_ms {
                    std::thread::sleep(StdDuration::from_millis(delay_ms));
                }
                let mut meta_rows = match meta_stmt
                    .query([])
                    .context("failed querying persisted rebuild row meta")
                {
                    Ok(rows) => rows,
                    Err(error) => {
                        send_finished(Err(error));
                        return;
                    }
                };
                let meta_row = match meta_rows
                    .next()
                    .context("failed stepping persisted rebuild row meta")
                {
                    Ok(row) => row,
                    Err(error) => {
                        send_finished(Err(error));
                        return;
                    }
                };
                snapshot.step_meta_elapsed_ms = Some(
                    step_meta_started_at
                        .elapsed()
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                );
                snapshot.row_exists = Some(meta_row.is_some());
                if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                    return;
                }
                let Some(meta_row) = meta_row else {
                    drop(meta_rows);
                    drop(meta_stmt);
                    let _ = tx.send(WorkerMessage::Finished(Ok(())));
                    return;
                };

                let _ = tx.send(WorkerMessage::Entered(
                    DiscoveryPersistedRebuildRowDriverCompareStage::ExtractPhase,
                ));
                let extract_phase_started_at = Instant::now();
                if let Some(delay_ms) = options.test_force_extract_phase_delay_ms {
                    std::thread::sleep(StdDuration::from_millis(delay_ms));
                }
                let phase = match meta_row
                    .get::<_, String>(0)
                    .context("failed extracting persisted rebuild phase")
                {
                    Ok(value) => value,
                    Err(error) => {
                        send_finished(Err(error));
                        return;
                    }
                };
                snapshot.extract_phase_elapsed_ms = Some(
                    extract_phase_started_at
                        .elapsed()
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                );
                snapshot.row_phase = Some(phase);
                if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                    return;
                }

                let _ = tx.send(WorkerMessage::Entered(
                    DiscoveryPersistedRebuildRowDriverCompareStage::ExtractUpdatedAt,
                ));
                let extract_updated_at_started_at = Instant::now();
                if let Some(delay_ms) = options.test_force_extract_updated_at_delay_ms {
                    std::thread::sleep(StdDuration::from_millis(delay_ms));
                }
                let updated_at = match meta_row
                    .get::<_, String>(1)
                    .context("failed extracting persisted rebuild updated_at")
                {
                    Ok(value) => value,
                    Err(error) => {
                        send_finished(Err(error));
                        return;
                    }
                };
                snapshot.extract_updated_at_elapsed_ms = Some(
                    extract_updated_at_started_at
                        .elapsed()
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                );
                snapshot.row_updated_at = Some(updated_at);
                if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                    return;
                }
            }
            drop(meta_stmt);

            if options.test_require_no_active_statements_before_prepare_size {
                let active_statement_count = store.sqlite_active_statement_count_for_debug();
                if active_statement_count != 0 {
                    send_finished(Err(anyhow!(
                        "expected no active statements before prepare_size, found {active_statement_count}"
                    )));
                    return;
                }
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowDriverCompareStage::PrepareSize,
            ));
            let prepare_size_started_at = Instant::now();
            if let Some(delay_ms) = options.test_force_prepare_size_delay_ms {
                std::thread::sleep(StdDuration::from_millis(delay_ms));
            }
            let mut size_stmt = match store
                .conn
                .prepare(
                    "SELECT length(CAST(state_json AS BLOB))
                     FROM discovery_persisted_rebuild_state
                     WHERE id = 1",
                )
                .context("failed preparing persisted rebuild row size query")
            {
                Ok(stmt) => stmt,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.prepare_size_elapsed_ms = Some(
                prepare_size_started_at
                    .elapsed()
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
            );
            if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                return;
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowDriverCompareStage::StepSize,
            ));
            let step_size_started_at = Instant::now();
            if let Some(delay_ms) = options.test_force_step_size_delay_ms {
                std::thread::sleep(StdDuration::from_millis(delay_ms));
            }
            let mut size_rows = match size_stmt
                .query([])
                .context("failed querying persisted rebuild row size")
            {
                Ok(rows) => rows,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            let size_row = match size_rows
                .next()
                .context("failed stepping persisted rebuild row size")
            {
                Ok(row) => row,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            let state_json_bytes = match size_row {
                Some(row) => match row
                    .get::<_, i64>(0)
                    .context("failed extracting persisted rebuild row size")
                {
                    Ok(value) => value.max(0) as usize,
                    Err(error) => {
                        send_finished(Err(error));
                        return;
                    }
                },
                None => 0,
            };
            snapshot.step_size_elapsed_ms = Some(
                step_size_started_at
                    .elapsed()
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
            );
            snapshot.row_state_json_bytes = Some(state_json_bytes);
            if tx.send(WorkerMessage::Snapshot(snapshot)).is_err() {
                return;
            }

            let _ = tx.send(WorkerMessage::Finished(Ok(())));
        });

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                diagnostic.budget_exhausted = true;
                diagnostic.total_elapsed_ms =
                    started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                diagnostic.skipped_stages = skipped_stages(diagnostic.stage, true);
                return Ok(diagnostic);
            }

            match rx.recv_timeout(remaining) {
                Ok(WorkerMessage::Entered(stage)) => {
                    diagnostic.stage = stage;
                }
                Ok(WorkerMessage::Snapshot(snapshot)) => {
                    diagnostic.open_db_elapsed_ms = snapshot.open_db_elapsed_ms;
                    diagnostic.load_connection_facts_elapsed_ms =
                        snapshot.load_connection_facts_elapsed_ms;
                    diagnostic.prepare_exists_elapsed_ms = snapshot.prepare_exists_elapsed_ms;
                    diagnostic.step_exists_elapsed_ms = snapshot.step_exists_elapsed_ms;
                    diagnostic.prepare_meta_elapsed_ms = snapshot.prepare_meta_elapsed_ms;
                    diagnostic.step_meta_elapsed_ms = snapshot.step_meta_elapsed_ms;
                    diagnostic.extract_phase_elapsed_ms = snapshot.extract_phase_elapsed_ms;
                    diagnostic.extract_updated_at_elapsed_ms =
                        snapshot.extract_updated_at_elapsed_ms;
                    diagnostic.prepare_size_elapsed_ms = snapshot.prepare_size_elapsed_ms;
                    diagnostic.step_size_elapsed_ms = snapshot.step_size_elapsed_ms;
                    diagnostic.row_exists = snapshot.row_exists;
                    diagnostic.row_phase = snapshot.row_phase;
                    diagnostic.row_updated_at = snapshot.row_updated_at;
                    diagnostic.row_state_json_bytes = snapshot.row_state_json_bytes;
                    diagnostic.connection_facts = snapshot.connection_facts;
                }
                Ok(WorkerMessage::Finished(result)) => {
                    if let Err(error) = result {
                        return Err(anyhow!(
                            "persisted rebuild row driver compare worker failed: {error}"
                        ));
                    }
                    diagnostic.stage = DiscoveryPersistedRebuildRowDriverCompareStage::Complete;
                    diagnostic.total_elapsed_ms =
                        started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    diagnostic.skipped_stages = Vec::new();
                    return Ok(diagnostic);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    diagnostic.budget_exhausted = true;
                    diagnostic.total_elapsed_ms =
                        started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    diagnostic.skipped_stages = skipped_stages(diagnostic.stage, true);
                    return Ok(diagnostic);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(anyhow!(
                        "persisted rebuild row driver compare worker disconnected before returning a result"
                    ));
                }
            }
        }
    }

    pub fn probe_discovery_persisted_rebuild_row_step_meta_compare_read_only(
        runtime_db_path: &Path,
        options: &DiscoveryPersistedRebuildRowStepMetaCompareOptions,
    ) -> Result<DiscoveryPersistedRebuildRowStepMetaCompareDiagnostic> {
        #[derive(Debug, Clone)]
        struct ProgressSnapshot {
            baseline_connection_facts: Option<crate::SqliteReadOnlyDriverCompareFacts>,
            shared_prepare_exists_elapsed_ms: Option<u64>,
            shared_step_exists_elapsed_ms: Option<u64>,
            shared_prepare_meta_elapsed_ms: Option<u64>,
            shared_step_meta_elapsed_ms: Option<u64>,
            shared_extract_phase_elapsed_ms: Option<u64>,
            shared_extract_updated_at_elapsed_ms: Option<u64>,
            shared_row_exists: Option<bool>,
            shared_row_phase: Option<String>,
            shared_row_updated_at: Option<String>,
            fresh_prepare_meta_elapsed_ms: Option<u64>,
            fresh_step_meta_elapsed_ms: Option<u64>,
            fresh_extract_phase_elapsed_ms: Option<u64>,
            fresh_extract_updated_at_elapsed_ms: Option<u64>,
            fresh_row_exists: Option<bool>,
            fresh_row_phase: Option<String>,
            fresh_row_updated_at: Option<String>,
            query_plus_next_variant_elapsed_ms: Option<u64>,
            query_row_variant_elapsed_ms: Option<u64>,
            query_only_on_elapsed_ms: Option<u64>,
            query_only_effective_query_only: Option<bool>,
            cache_tuned_elapsed_ms: Option<u64>,
            cache_tuned_effective_cache_size: Option<i64>,
            mmap_tuned_elapsed_ms: Option<u64>,
            mmap_tuned_effective_mmap_size: Option<i64>,
        }

        impl ProgressSnapshot {
            fn new() -> Self {
                Self {
                    baseline_connection_facts: None,
                    shared_prepare_exists_elapsed_ms: None,
                    shared_step_exists_elapsed_ms: None,
                    shared_prepare_meta_elapsed_ms: None,
                    shared_step_meta_elapsed_ms: None,
                    shared_extract_phase_elapsed_ms: None,
                    shared_extract_updated_at_elapsed_ms: None,
                    shared_row_exists: None,
                    shared_row_phase: None,
                    shared_row_updated_at: None,
                    fresh_prepare_meta_elapsed_ms: None,
                    fresh_step_meta_elapsed_ms: None,
                    fresh_extract_phase_elapsed_ms: None,
                    fresh_extract_updated_at_elapsed_ms: None,
                    fresh_row_exists: None,
                    fresh_row_phase: None,
                    fresh_row_updated_at: None,
                    query_plus_next_variant_elapsed_ms: None,
                    query_row_variant_elapsed_ms: None,
                    query_only_on_elapsed_ms: None,
                    query_only_effective_query_only: None,
                    cache_tuned_elapsed_ms: None,
                    cache_tuned_effective_cache_size: None,
                    mmap_tuned_elapsed_ms: None,
                    mmap_tuned_effective_mmap_size: None,
                }
            }
        }

        #[derive(Debug)]
        enum WorkerMessage {
            Entered(DiscoveryPersistedRebuildRowStepMetaCompareStage),
            Snapshot(ProgressSnapshot),
            Finished(Result<ProgressSnapshot, String>),
        }

        fn skipped_stages(
            current_stage: DiscoveryPersistedRebuildRowStepMetaCompareStage,
            budget_exhausted: bool,
        ) -> Vec<DiscoveryPersistedRebuildRowStepMetaCompareStage> {
            if !budget_exhausted {
                return Vec::new();
            }
            let relevant = [
                DiscoveryPersistedRebuildRowStepMetaCompareStage::OpenSharedConnection,
                DiscoveryPersistedRebuildRowStepMetaCompareStage::SharedConnectionCurrentPath,
                DiscoveryPersistedRebuildRowStepMetaCompareStage::FreshConnectionQueryPlusNext,
                DiscoveryPersistedRebuildRowStepMetaCompareStage::FreshConnectionQueryRowVariant,
                DiscoveryPersistedRebuildRowStepMetaCompareStage::FreshConnectionQueryOnlyVariant,
                DiscoveryPersistedRebuildRowStepMetaCompareStage::FreshConnectionCacheTunedVariant,
                DiscoveryPersistedRebuildRowStepMetaCompareStage::FreshConnectionMmapTunedVariant,
                DiscoveryPersistedRebuildRowStepMetaCompareStage::Complete,
            ];
            let cutoff_index = relevant
                .iter()
                .position(|stage| *stage == current_stage)
                .unwrap_or(relevant.len());
            relevant
                .into_iter()
                .skip(cutoff_index.saturating_add(1))
                .collect()
        }

        let started_at = Instant::now();
        let deadline = started_at + StdDuration::from_millis(options.budget_ms);
        let mut diagnostic = DiscoveryPersistedRebuildRowStepMetaCompareDiagnostic {
            stage: DiscoveryPersistedRebuildRowStepMetaCompareStage::OpenSharedConnection,
            budget_exhausted: false,
            skipped_stages: Vec::new(),
            total_elapsed_ms: 0,
            progress_snapshots_emitted: false,
            baseline_connection_facts: None,
            shared_prepare_exists_elapsed_ms: None,
            shared_step_exists_elapsed_ms: None,
            shared_prepare_meta_elapsed_ms: None,
            shared_step_meta_elapsed_ms: None,
            shared_extract_phase_elapsed_ms: None,
            shared_extract_updated_at_elapsed_ms: None,
            shared_row_exists: None,
            shared_row_phase: None,
            shared_row_updated_at: None,
            fresh_prepare_meta_elapsed_ms: None,
            fresh_step_meta_elapsed_ms: None,
            fresh_extract_phase_elapsed_ms: None,
            fresh_extract_updated_at_elapsed_ms: None,
            fresh_row_exists: None,
            fresh_row_phase: None,
            fresh_row_updated_at: None,
            query_plus_next_variant_elapsed_ms: None,
            query_row_variant_elapsed_ms: None,
            query_only_on_elapsed_ms: None,
            query_only_effective_query_only: None,
            cache_tuned_elapsed_ms: None,
            cache_tuned_effective_cache_size: None,
            mmap_tuned_elapsed_ms: None,
            mmap_tuned_effective_mmap_size: None,
        };
        let (tx, rx) = mpsc::sync_channel(16);
        let runtime_db_path = runtime_db_path.to_path_buf();
        let options = options.clone();
        std::thread::spawn(move || {
            let send_finished = |result: Result<ProgressSnapshot, anyhow::Error>| {
                let _ = tx.send(WorkerMessage::Finished(
                    result.map_err(|error| format!("{error:#}")),
                ));
            };
            let send_snapshot = |snapshot: &ProgressSnapshot| {
                if options.test_disable_progress_snapshots {
                    return true;
                }
                tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_ok()
            };

            let mut snapshot = ProgressSnapshot::new();

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowStepMetaCompareStage::OpenSharedConnection,
            ));
            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowStepMetaCompareStage::SharedConnectionCurrentPath,
            ));
            let shared_variant = match Self::run_isolated_step_meta_detail_shared_path(
                &runtime_db_path,
                options.test_force_shared_step_meta_delay_ms,
            ) {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.baseline_connection_facts = Some(shared_variant.connection_facts);
            snapshot.shared_prepare_exists_elapsed_ms =
                Some(shared_variant.exists_probe.prepare_exists_elapsed_ms);
            snapshot.shared_step_exists_elapsed_ms =
                Some(shared_variant.exists_probe.step_exists_elapsed_ms);
            snapshot.shared_row_exists = Some(shared_variant.query_result.row_exists);
            let shared_row_exists = shared_variant.query_result.row_exists;
            if shared_row_exists {
                snapshot.shared_prepare_meta_elapsed_ms =
                    Some(shared_variant.query_result.prepare_meta_elapsed_ms);
                snapshot.shared_step_meta_elapsed_ms =
                    Some(shared_variant.query_result.step_meta_elapsed_ms);
                snapshot.shared_extract_phase_elapsed_ms =
                    Some(shared_variant.query_result.extract_phase_elapsed_ms);
                snapshot.shared_extract_updated_at_elapsed_ms =
                    Some(shared_variant.query_result.extract_updated_at_elapsed_ms);
                snapshot.shared_row_phase = shared_variant.query_result.row_phase;
                snapshot.shared_row_updated_at = shared_variant.query_result.row_updated_at;
            }
            if !send_snapshot(&snapshot) {
                return;
            }
            if options.test_truncate_after_shared_section {
                let _ = tx.send(WorkerMessage::Finished(Ok(snapshot)));
                return;
            }

            if !shared_row_exists {
                let _ = tx.send(WorkerMessage::Finished(Ok(snapshot)));
                return;
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowStepMetaCompareStage::FreshConnectionQueryPlusNext,
            ));
            let (fresh_store, _) =
                match Self::open_probe_store_with_compare_facts(&runtime_db_path, None, None, None)
                {
                    Ok(value) => value,
                    Err(error) => {
                        send_finished(Err(error));
                        return;
                    }
                };
            let fresh_variant = match Self::run_query_plus_next_variant_with_extracts(
                &fresh_store,
                options.test_force_fresh_connection_query_delay_ms,
            ) {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.fresh_prepare_meta_elapsed_ms = Some(fresh_variant.prepare_meta_elapsed_ms);
            snapshot.fresh_step_meta_elapsed_ms = Some(fresh_variant.step_meta_elapsed_ms);
            snapshot.fresh_extract_phase_elapsed_ms = Some(fresh_variant.extract_phase_elapsed_ms);
            snapshot.fresh_extract_updated_at_elapsed_ms =
                Some(fresh_variant.extract_updated_at_elapsed_ms);
            snapshot.fresh_row_exists = Some(fresh_variant.row_exists);
            snapshot.fresh_row_phase = fresh_variant.row_phase;
            snapshot.fresh_row_updated_at = fresh_variant.row_updated_at;
            snapshot.query_plus_next_variant_elapsed_ms = Some(fresh_variant.total_elapsed_ms);
            if !send_snapshot(&snapshot) {
                return;
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowStepMetaCompareStage::FreshConnectionQueryRowVariant,
            ));
            let (query_row_store, _) =
                match Self::open_probe_store_with_compare_facts(&runtime_db_path, None, None, None)
                {
                    Ok(value) => value,
                    Err(error) => {
                        send_finished(Err(error));
                        return;
                    }
                };
            let query_row_variant = match Self::run_query_row_variant(
                &query_row_store,
                options.test_force_fresh_connection_query_delay_ms,
            ) {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.query_row_variant_elapsed_ms = Some(query_row_variant.total_elapsed_ms);
            if !send_snapshot(&snapshot) {
                return;
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowStepMetaCompareStage::FreshConnectionQueryOnlyVariant,
            ));
            let (query_only_store, query_only_facts) =
                match Self::open_probe_store_with_compare_facts(
                    &runtime_db_path,
                    Some(true),
                    None,
                    None,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        send_finished(Err(error));
                        return;
                    }
                };
            let query_only_variant = match Self::run_query_plus_next_variant_with_extracts(
                &query_only_store,
                options.test_force_fresh_connection_query_delay_ms,
            ) {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.query_only_on_elapsed_ms = Some(query_only_variant.total_elapsed_ms);
            snapshot.query_only_effective_query_only = Some(query_only_facts.query_only);
            if !send_snapshot(&snapshot) {
                return;
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowStepMetaCompareStage::FreshConnectionCacheTunedVariant,
            ));
            let (cache_tuned_store, cache_tuned_facts) =
                match Self::open_probe_store_with_compare_facts(
                    &runtime_db_path,
                    None,
                    Some(-131_072),
                    None,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        send_finished(Err(error));
                        return;
                    }
                };
            let cache_tuned_variant = match Self::run_query_plus_next_variant_with_extracts(
                &cache_tuned_store,
                options.test_force_fresh_connection_query_delay_ms,
            ) {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.cache_tuned_elapsed_ms = Some(cache_tuned_variant.total_elapsed_ms);
            snapshot.cache_tuned_effective_cache_size = Some(cache_tuned_facts.cache_size);
            if !send_snapshot(&snapshot) {
                return;
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowStepMetaCompareStage::FreshConnectionMmapTunedVariant,
            ));
            let (mmap_tuned_store, mmap_tuned_facts) =
                match Self::open_probe_store_with_compare_facts(
                    &runtime_db_path,
                    None,
                    None,
                    Some(268_435_456),
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        send_finished(Err(error));
                        return;
                    }
                };
            let mmap_tuned_variant = match Self::run_query_plus_next_variant_with_extracts(
                &mmap_tuned_store,
                options.test_force_fresh_connection_query_delay_ms,
            ) {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.mmap_tuned_elapsed_ms = Some(mmap_tuned_variant.total_elapsed_ms);
            snapshot.mmap_tuned_effective_mmap_size = Some(mmap_tuned_facts.mmap_size);
            if !send_snapshot(&snapshot) {
                return;
            }

            let _ = tx.send(WorkerMessage::Finished(Ok(snapshot)));
        });

        let apply_snapshot =
            |diagnostic: &mut DiscoveryPersistedRebuildRowStepMetaCompareDiagnostic,
             snapshot: ProgressSnapshot| {
                diagnostic.baseline_connection_facts = snapshot.baseline_connection_facts;
                diagnostic.shared_prepare_exists_elapsed_ms =
                    snapshot.shared_prepare_exists_elapsed_ms;
                diagnostic.shared_step_exists_elapsed_ms = snapshot.shared_step_exists_elapsed_ms;
                diagnostic.shared_prepare_meta_elapsed_ms = snapshot.shared_prepare_meta_elapsed_ms;
                diagnostic.shared_step_meta_elapsed_ms = snapshot.shared_step_meta_elapsed_ms;
                diagnostic.shared_extract_phase_elapsed_ms =
                    snapshot.shared_extract_phase_elapsed_ms;
                diagnostic.shared_extract_updated_at_elapsed_ms =
                    snapshot.shared_extract_updated_at_elapsed_ms;
                diagnostic.shared_row_exists = snapshot.shared_row_exists;
                diagnostic.shared_row_phase = snapshot.shared_row_phase;
                diagnostic.shared_row_updated_at = snapshot.shared_row_updated_at;
                diagnostic.fresh_prepare_meta_elapsed_ms = snapshot.fresh_prepare_meta_elapsed_ms;
                diagnostic.fresh_step_meta_elapsed_ms = snapshot.fresh_step_meta_elapsed_ms;
                diagnostic.fresh_extract_phase_elapsed_ms = snapshot.fresh_extract_phase_elapsed_ms;
                diagnostic.fresh_extract_updated_at_elapsed_ms =
                    snapshot.fresh_extract_updated_at_elapsed_ms;
                diagnostic.fresh_row_exists = snapshot.fresh_row_exists;
                diagnostic.fresh_row_phase = snapshot.fresh_row_phase;
                diagnostic.fresh_row_updated_at = snapshot.fresh_row_updated_at;
                diagnostic.query_plus_next_variant_elapsed_ms =
                    snapshot.query_plus_next_variant_elapsed_ms;
                diagnostic.query_row_variant_elapsed_ms = snapshot.query_row_variant_elapsed_ms;
                diagnostic.query_only_on_elapsed_ms = snapshot.query_only_on_elapsed_ms;
                diagnostic.query_only_effective_query_only =
                    snapshot.query_only_effective_query_only;
                diagnostic.cache_tuned_elapsed_ms = snapshot.cache_tuned_elapsed_ms;
                diagnostic.cache_tuned_effective_cache_size =
                    snapshot.cache_tuned_effective_cache_size;
                diagnostic.mmap_tuned_elapsed_ms = snapshot.mmap_tuned_elapsed_ms;
                diagnostic.mmap_tuned_effective_mmap_size = snapshot.mmap_tuned_effective_mmap_size;
            };

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                diagnostic.budget_exhausted = true;
                diagnostic.total_elapsed_ms =
                    started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                diagnostic.skipped_stages = skipped_stages(diagnostic.stage, true);
                return Ok(diagnostic);
            }

            match rx.recv_timeout(remaining) {
                Ok(WorkerMessage::Entered(stage)) => {
                    diagnostic.stage = stage;
                }
                Ok(WorkerMessage::Snapshot(snapshot)) => {
                    diagnostic.progress_snapshots_emitted = true;
                    apply_snapshot(&mut diagnostic, snapshot);
                }
                Ok(WorkerMessage::Finished(result)) => {
                    let snapshot = match result {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            return Err(anyhow!(
                                "persisted rebuild row step-meta compare worker failed: {error}"
                            ));
                        }
                    };
                    apply_snapshot(&mut diagnostic, snapshot);
                    diagnostic.stage = DiscoveryPersistedRebuildRowStepMetaCompareStage::Complete;
                    diagnostic.total_elapsed_ms =
                        started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    diagnostic.skipped_stages = Vec::new();
                    return Ok(diagnostic);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    diagnostic.budget_exhausted = true;
                    diagnostic.total_elapsed_ms =
                        started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    diagnostic.skipped_stages = skipped_stages(diagnostic.stage, true);
                    return Ok(diagnostic);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(anyhow!(
                        "persisted rebuild row step-meta compare worker disconnected before returning a result"
                    ));
                }
            }
        }
    }

    fn open_probe_store_with_compare_facts(
        runtime_db_path: &Path,
        query_only: Option<bool>,
        cache_size: Option<i64>,
        mmap_size: Option<i64>,
    ) -> Result<(SqliteStore, crate::SqliteReadOnlyDriverCompareFacts)> {
        let store = SqliteStore::open_read_only(runtime_db_path).with_context(|| {
            format!(
                "failed opening runtime sqlite db read-only {}",
                runtime_db_path.display()
            )
        })?;
        if let Some(query_only) = query_only {
            store
                .conn
                .pragma_update(None, "query_only", if query_only { 1 } else { 0 })
                .context("failed setting sqlite query_only for compare probe")?;
        }
        if let Some(cache_size) = cache_size {
            store
                .conn
                .pragma_update(None, "cache_size", cache_size)
                .context("failed setting sqlite cache_size for compare probe")?;
        }
        if let Some(mmap_size) = mmap_size {
            store
                .conn
                .pragma_update(None, "mmap_size", mmap_size)
                .context("failed setting sqlite mmap_size for compare probe")?;
        }
        let facts = store.sqlite_read_only_driver_compare_facts()?;
        Ok((store, facts))
    }

    fn run_persisted_rebuild_row_exists_probe(
        store: &SqliteStore,
    ) -> Result<PersistedRebuildRowExistsProbeResult> {
        let prepare_exists_started_at = Instant::now();
        let mut exists_stmt = store
            .conn
            .prepare("SELECT 1 FROM discovery_persisted_rebuild_state WHERE id = 1")
            .context("failed preparing persisted rebuild exists probe")?;
        let prepare_exists_elapsed_ms = prepare_exists_started_at
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;

        let step_exists_started_at = Instant::now();
        let row_exists = {
            let mut exists_rows = exists_stmt
                .query([])
                .context("failed querying persisted rebuild exists probe")?;
            exists_rows
                .next()
                .context("failed stepping persisted rebuild exists probe")?
                .is_some()
        };
        let step_exists_elapsed_ms = step_exists_started_at
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        drop(exists_stmt);

        Ok(PersistedRebuildRowExistsProbeResult {
            prepare_exists_elapsed_ms,
            step_exists_elapsed_ms,
            row_exists,
        })
    }

    fn run_query_plus_next_variant_with_extracts(
        store: &SqliteStore,
        force_step_delay_ms: Option<u64>,
    ) -> Result<PersistedRebuildRowQueryPlusNextWithExtractsResult> {
        let total_started_at = Instant::now();
        let prepare_meta_started_at = Instant::now();
        let mut meta_stmt = store
            .conn
            .prepare(
                "SELECT phase, updated_at
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
            )
            .context("failed preparing persisted rebuild row meta query")?;
        let prepare_meta_elapsed_ms = prepare_meta_started_at
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;

        let step_meta_started_at = Instant::now();
        if let Some(delay_ms) = force_step_delay_ms {
            std::thread::sleep(StdDuration::from_millis(delay_ms));
        }
        let mut meta_rows = meta_stmt
            .query([])
            .context("failed querying persisted rebuild row meta")?;
        let meta_row = meta_rows
            .next()
            .context("failed stepping persisted rebuild row meta")?;
        let step_meta_elapsed_ms = step_meta_started_at
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let Some(meta_row) = meta_row else {
            drop(meta_rows);
            drop(meta_stmt);
            return Ok(PersistedRebuildRowQueryPlusNextWithExtractsResult {
                prepare_meta_elapsed_ms,
                step_meta_elapsed_ms,
                extract_phase_elapsed_ms: 0,
                extract_updated_at_elapsed_ms: 0,
                total_elapsed_ms: total_started_at.elapsed().as_millis().min(u64::MAX as u128)
                    as u64,
                row_exists: false,
                row_phase: None,
                row_updated_at: None,
            });
        };

        let extract_phase_started_at = Instant::now();
        let phase = meta_row
            .get::<_, String>(0)
            .context("failed extracting persisted rebuild phase")?;
        let extract_phase_elapsed_ms = extract_phase_started_at
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;

        let extract_updated_at_started_at = Instant::now();
        let updated_at = meta_row
            .get::<_, String>(1)
            .context("failed extracting persisted rebuild updated_at")?;
        let extract_updated_at_elapsed_ms = extract_updated_at_started_at
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;

        drop(meta_rows);
        drop(meta_stmt);

        Ok(PersistedRebuildRowQueryPlusNextWithExtractsResult {
            prepare_meta_elapsed_ms,
            step_meta_elapsed_ms,
            extract_phase_elapsed_ms,
            extract_updated_at_elapsed_ms,
            total_elapsed_ms: total_started_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
            row_exists: true,
            row_phase: Some(phase),
            row_updated_at: Some(updated_at),
        })
    }

    fn run_query_plus_next_variant_step_only(
        store: &SqliteStore,
        force_step_delay_ms: Option<u64>,
    ) -> Result<PersistedRebuildRowQueryPlusNextStepOnlyResult> {
        let mut meta_stmt = store
            .conn
            .prepare(
                "SELECT phase, updated_at
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
            )
            .context("failed preparing persisted rebuild row meta query")?;
        let step_meta_started_at = Instant::now();
        if let Some(delay_ms) = force_step_delay_ms {
            std::thread::sleep(StdDuration::from_millis(delay_ms));
        }
        let mut meta_rows = meta_stmt
            .query([])
            .context("failed querying persisted rebuild row meta")?;
        let meta_row = meta_rows
            .next()
            .context("failed stepping persisted rebuild row meta")?;
        let step_meta_elapsed_ms = step_meta_started_at
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let row_exists = meta_row.is_some();
        drop(meta_rows);
        drop(meta_stmt);
        Ok(PersistedRebuildRowQueryPlusNextStepOnlyResult {
            step_meta_elapsed_ms,
            row_exists,
        })
    }

    fn run_query_row_variant(
        store: &SqliteStore,
        force_delay_ms: Option<u64>,
    ) -> Result<QueryRowVariantResult> {
        let started_at = Instant::now();
        if let Some(delay_ms) = force_delay_ms {
            std::thread::sleep(StdDuration::from_millis(delay_ms));
        }
        let row = store
            .conn
            .query_row(
                "SELECT phase, updated_at
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .context("failed executing persisted rebuild row meta query_row variant")?;
        let _row = row;
        Ok(QueryRowVariantResult {
            total_elapsed_ms: started_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
        })
    }

    fn run_isolated_step_meta_detail_shared_path(
        runtime_db_path: &Path,
        force_step_delay_ms: Option<u64>,
    ) -> Result<PersistedRebuildRowStepMetaSharedPathResult> {
        let (store, connection_facts) =
            Self::open_probe_store_with_compare_facts(runtime_db_path, None, None, None)?;
        let exists_probe = Self::run_persisted_rebuild_row_exists_probe(&store)?;
        let query_result = if !exists_probe.row_exists {
            PersistedRebuildRowQueryPlusNextWithExtractsResult {
                prepare_meta_elapsed_ms: 0,
                step_meta_elapsed_ms: 0,
                extract_phase_elapsed_ms: 0,
                extract_updated_at_elapsed_ms: 0,
                total_elapsed_ms: 0,
                row_exists: false,
                row_phase: None,
                row_updated_at: None,
            }
        } else {
            Self::run_query_plus_next_variant_with_extracts(&store, force_step_delay_ms)?
        };
        Ok(PersistedRebuildRowStepMetaSharedPathResult {
            connection_facts,
            exists_probe,
            query_result,
            loads_connection_facts_before_meta_query: true,
            uses_query_plus_next: true,
            finalizes_exists_before_prepare_meta: true,
            extracts_phase_and_updated_at_after_step: true,
            measures_prepare_meta_separately: true,
            measures_extract_separately: true,
        })
    }

    pub fn probe_discovery_persisted_rebuild_row_step_meta_isolated_shared_read_only(
        runtime_db_path: &Path,
        options: &DiscoveryPersistedRebuildRowStepMetaIsolatedSharedOptions,
    ) -> Result<DiscoveryPersistedRebuildRowStepMetaIsolatedSharedDiagnostic> {
        #[derive(Debug)]
        enum WorkerMessage {
            Finished(Result<PersistedRebuildRowStepMetaSharedPathResult, String>),
        }

        let started_at = Instant::now();
        let deadline = started_at + StdDuration::from_millis(options.budget_ms);
        let mut diagnostic = DiscoveryPersistedRebuildRowStepMetaIsolatedSharedDiagnostic {
            stage: DiscoveryPersistedRebuildRowStepMetaIsolatedSharedStage::SharedPath,
            budget_exhausted: false,
            total_elapsed_ms: 0,
            prepare_exists_elapsed_ms: None,
            step_exists_elapsed_ms: None,
            prepare_meta_elapsed_ms: None,
            step_meta_elapsed_ms: None,
            extract_phase_elapsed_ms: None,
            extract_updated_at_elapsed_ms: None,
            row_exists: None,
            row_phase: None,
            row_updated_at: None,
            loads_connection_facts_before_meta_query: false,
            uses_query_plus_next: false,
            finalizes_exists_before_prepare_meta: false,
            extracts_phase_and_updated_at_after_step: false,
            measures_prepare_meta_separately: false,
            measures_extract_separately: false,
        };
        let (tx, rx) = mpsc::sync_channel(1);
        let runtime_db_path = runtime_db_path.to_path_buf();
        let options = options.clone();
        std::thread::spawn(move || {
            let result = Self::run_isolated_step_meta_detail_shared_path(
                &runtime_db_path,
                options.test_force_shared_step_meta_delay_ms,
            )
            .map_err(|error| format!("{error:#}"));
            let _ = tx.send(WorkerMessage::Finished(result));
        });

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            diagnostic.budget_exhausted = true;
            diagnostic.total_elapsed_ms =
                started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
            return Ok(diagnostic);
        }

        match rx.recv_timeout(remaining) {
            Ok(WorkerMessage::Finished(result)) => {
                let result = result.map_err(|error| {
                    anyhow!("persisted rebuild row isolated shared helper worker failed: {error}")
                })?;
                diagnostic.stage = DiscoveryPersistedRebuildRowStepMetaIsolatedSharedStage::Complete;
                diagnostic.total_elapsed_ms =
                    started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                diagnostic.prepare_exists_elapsed_ms =
                    Some(result.exists_probe.prepare_exists_elapsed_ms);
                diagnostic.step_exists_elapsed_ms =
                    Some(result.exists_probe.step_exists_elapsed_ms);
                diagnostic.row_exists = Some(result.query_result.row_exists);
                diagnostic.loads_connection_facts_before_meta_query =
                    result.loads_connection_facts_before_meta_query;
                diagnostic.uses_query_plus_next = result.uses_query_plus_next;
                diagnostic.finalizes_exists_before_prepare_meta =
                    result.finalizes_exists_before_prepare_meta;
                diagnostic.extracts_phase_and_updated_at_after_step =
                    result.extracts_phase_and_updated_at_after_step;
                diagnostic.measures_prepare_meta_separately =
                    result.measures_prepare_meta_separately;
                diagnostic.measures_extract_separately =
                    result.measures_extract_separately;
                if result.query_result.row_exists {
                    diagnostic.prepare_meta_elapsed_ms =
                        Some(result.query_result.prepare_meta_elapsed_ms);
                    diagnostic.step_meta_elapsed_ms =
                        Some(result.query_result.step_meta_elapsed_ms);
                    diagnostic.extract_phase_elapsed_ms =
                        Some(result.query_result.extract_phase_elapsed_ms);
                    diagnostic.extract_updated_at_elapsed_ms =
                        Some(result.query_result.extract_updated_at_elapsed_ms);
                    diagnostic.row_phase = result.query_result.row_phase;
                    diagnostic.row_updated_at = result.query_result.row_updated_at;
                }
                Ok(diagnostic)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                diagnostic.budget_exhausted = true;
                diagnostic.total_elapsed_ms =
                    started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                Ok(diagnostic)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(anyhow!(
                "persisted rebuild row isolated shared helper worker disconnected before returning a result"
            )),
        }
    }

    fn run_isolated_shared_sequence_baseline_path(
        runtime_db_path: &Path,
        force_step_delay_ms: Option<u64>,
    ) -> Result<PersistedRebuildRowSharedSequenceBaselinePathResult> {
        let (store, connection_facts) =
            Self::open_probe_store_with_compare_facts(runtime_db_path, None, None, None)?;
        let exists_probe = Self::run_persisted_rebuild_row_exists_probe(&store)?;
        if !exists_probe.row_exists {
            return Ok(PersistedRebuildRowSharedSequenceBaselinePathResult {
                connection_facts,
                exists_probe,
                query_result: PersistedRebuildRowQueryPlusNextStepOnlyResult {
                    step_meta_elapsed_ms: 0,
                    row_exists: false,
                },
                loads_connection_facts_before_meta_query: true,
                uses_query_plus_next: true,
                finalizes_exists_before_prepare_meta: true,
                extracts_phase_and_updated_at_after_step: false,
                measures_prepare_meta_separately: false,
                measures_extract_separately: false,
            });
        }
        let query_result =
            Self::run_query_plus_next_variant_step_only(&store, force_step_delay_ms)?;
        Ok(PersistedRebuildRowSharedSequenceBaselinePathResult {
            connection_facts,
            exists_probe,
            query_result,
            loads_connection_facts_before_meta_query: true,
            uses_query_plus_next: true,
            finalizes_exists_before_prepare_meta: true,
            extracts_phase_and_updated_at_after_step: false,
            measures_prepare_meta_separately: false,
            measures_extract_separately: false,
        })
    }

    pub fn probe_discovery_persisted_rebuild_row_shared_path_diff_read_only(
        runtime_db_path: &Path,
        options: &DiscoveryPersistedRebuildRowSharedPathDiffOptions,
    ) -> Result<DiscoveryPersistedRebuildRowSharedPathDiffDiagnostic> {
        #[derive(Debug, Clone)]
        struct ProgressSnapshot {
            step_meta_detail_prepare_exists_elapsed_ms: Option<u64>,
            step_meta_detail_step_exists_elapsed_ms: Option<u64>,
            step_meta_detail_prepare_meta_elapsed_ms: Option<u64>,
            step_meta_detail_step_meta_elapsed_ms: Option<u64>,
            step_meta_detail_extract_phase_elapsed_ms: Option<u64>,
            step_meta_detail_extract_updated_at_elapsed_ms: Option<u64>,
            step_meta_detail_row_exists: Option<bool>,
            shared_sequence_prepare_exists_elapsed_ms: Option<u64>,
            shared_sequence_step_exists_elapsed_ms: Option<u64>,
            shared_sequence_step_meta_elapsed_ms: Option<u64>,
            shared_sequence_row_exists: Option<bool>,
            step_meta_detail_loads_connection_facts_before_meta_query: bool,
            shared_sequence_loads_connection_facts_before_meta_query: bool,
            step_meta_detail_uses_query_plus_next: bool,
            shared_sequence_uses_query_plus_next: bool,
            step_meta_detail_finalizes_exists_before_prepare_meta: bool,
            shared_sequence_finalizes_exists_before_prepare_meta: bool,
            step_meta_detail_extracts_phase_and_updated_at_after_step: bool,
            shared_sequence_extracts_phase_and_updated_at_after_step: bool,
            step_meta_detail_measures_prepare_meta_separately: bool,
            shared_sequence_measures_prepare_meta_separately: bool,
            step_meta_detail_measures_extract_separately: bool,
            shared_sequence_measures_extract_separately: bool,
        }

        impl ProgressSnapshot {
            fn new() -> Self {
                Self {
                    step_meta_detail_prepare_exists_elapsed_ms: None,
                    step_meta_detail_step_exists_elapsed_ms: None,
                    step_meta_detail_prepare_meta_elapsed_ms: None,
                    step_meta_detail_step_meta_elapsed_ms: None,
                    step_meta_detail_extract_phase_elapsed_ms: None,
                    step_meta_detail_extract_updated_at_elapsed_ms: None,
                    step_meta_detail_row_exists: None,
                    shared_sequence_prepare_exists_elapsed_ms: None,
                    shared_sequence_step_exists_elapsed_ms: None,
                    shared_sequence_step_meta_elapsed_ms: None,
                    shared_sequence_row_exists: None,
                    step_meta_detail_loads_connection_facts_before_meta_query: false,
                    shared_sequence_loads_connection_facts_before_meta_query: false,
                    step_meta_detail_uses_query_plus_next: false,
                    shared_sequence_uses_query_plus_next: false,
                    step_meta_detail_finalizes_exists_before_prepare_meta: false,
                    shared_sequence_finalizes_exists_before_prepare_meta: false,
                    step_meta_detail_extracts_phase_and_updated_at_after_step: false,
                    shared_sequence_extracts_phase_and_updated_at_after_step: false,
                    step_meta_detail_measures_prepare_meta_separately: false,
                    shared_sequence_measures_prepare_meta_separately: false,
                    step_meta_detail_measures_extract_separately: false,
                    shared_sequence_measures_extract_separately: false,
                }
            }
        }

        #[derive(Debug)]
        enum WorkerMessage {
            Entered(DiscoveryPersistedRebuildRowSharedPathDiffStage),
            Snapshot(ProgressSnapshot),
            Finished(Result<(), String>),
        }

        let started_at = Instant::now();
        let deadline = started_at + StdDuration::from_millis(options.budget_ms);
        let mut diagnostic = DiscoveryPersistedRebuildRowSharedPathDiffDiagnostic {
            stage: DiscoveryPersistedRebuildRowSharedPathDiffStage::StepMetaDetailSharedPath,
            budget_exhausted: false,
            total_elapsed_ms: 0,
            step_meta_detail_prepare_exists_elapsed_ms: None,
            step_meta_detail_step_exists_elapsed_ms: None,
            step_meta_detail_prepare_meta_elapsed_ms: None,
            step_meta_detail_step_meta_elapsed_ms: None,
            step_meta_detail_extract_phase_elapsed_ms: None,
            step_meta_detail_extract_updated_at_elapsed_ms: None,
            step_meta_detail_row_exists: None,
            shared_sequence_prepare_exists_elapsed_ms: None,
            shared_sequence_step_exists_elapsed_ms: None,
            shared_sequence_step_meta_elapsed_ms: None,
            shared_sequence_row_exists: None,
            step_meta_detail_loads_connection_facts_before_meta_query: false,
            shared_sequence_loads_connection_facts_before_meta_query: false,
            step_meta_detail_uses_query_plus_next: false,
            shared_sequence_uses_query_plus_next: false,
            step_meta_detail_finalizes_exists_before_prepare_meta: false,
            shared_sequence_finalizes_exists_before_prepare_meta: false,
            step_meta_detail_extracts_phase_and_updated_at_after_step: false,
            shared_sequence_extracts_phase_and_updated_at_after_step: false,
            step_meta_detail_measures_prepare_meta_separately: false,
            shared_sequence_measures_prepare_meta_separately: false,
            step_meta_detail_measures_extract_separately: false,
            shared_sequence_measures_extract_separately: false,
        };
        let (tx, rx) = mpsc::sync_channel(8);
        let runtime_db_path = runtime_db_path.to_path_buf();
        let options = options.clone();
        std::thread::spawn(move || {
            let send_finished = |result: Result<(), anyhow::Error>| {
                let _ = tx.send(WorkerMessage::Finished(
                    result.map_err(|error| format!("{error:#}")),
                ));
            };

            let mut snapshot = ProgressSnapshot::new();

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowSharedPathDiffStage::StepMetaDetailSharedPath,
            ));
            let old_shared = match Self::run_isolated_step_meta_detail_shared_path(
                &runtime_db_path,
                options.test_force_step_meta_detail_shared_step_meta_delay_ms,
            ) {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.step_meta_detail_prepare_exists_elapsed_ms =
                Some(old_shared.exists_probe.prepare_exists_elapsed_ms);
            snapshot.step_meta_detail_step_exists_elapsed_ms =
                Some(old_shared.exists_probe.step_exists_elapsed_ms);
            snapshot.step_meta_detail_row_exists = Some(old_shared.query_result.row_exists);
            snapshot.step_meta_detail_loads_connection_facts_before_meta_query =
                old_shared.loads_connection_facts_before_meta_query;
            snapshot.step_meta_detail_uses_query_plus_next = old_shared.uses_query_plus_next;
            snapshot.step_meta_detail_finalizes_exists_before_prepare_meta =
                old_shared.finalizes_exists_before_prepare_meta;
            snapshot.step_meta_detail_extracts_phase_and_updated_at_after_step =
                old_shared.extracts_phase_and_updated_at_after_step;
            snapshot.step_meta_detail_measures_prepare_meta_separately =
                old_shared.measures_prepare_meta_separately;
            snapshot.step_meta_detail_measures_extract_separately =
                old_shared.measures_extract_separately;
            if old_shared.query_result.row_exists {
                snapshot.step_meta_detail_prepare_meta_elapsed_ms =
                    Some(old_shared.query_result.prepare_meta_elapsed_ms);
                snapshot.step_meta_detail_step_meta_elapsed_ms =
                    Some(old_shared.query_result.step_meta_elapsed_ms);
                snapshot.step_meta_detail_extract_phase_elapsed_ms =
                    Some(old_shared.query_result.extract_phase_elapsed_ms);
                snapshot.step_meta_detail_extract_updated_at_elapsed_ms =
                    Some(old_shared.query_result.extract_updated_at_elapsed_ms);
            }
            if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                return;
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowSharedPathDiffStage::SharedSequenceBaselinePath,
            ));
            let new_shared = match Self::run_isolated_shared_sequence_baseline_path(
                &runtime_db_path,
                options.test_force_shared_sequence_baseline_step_meta_delay_ms,
            ) {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.shared_sequence_prepare_exists_elapsed_ms =
                Some(new_shared.exists_probe.prepare_exists_elapsed_ms);
            snapshot.shared_sequence_step_exists_elapsed_ms =
                Some(new_shared.exists_probe.step_exists_elapsed_ms);
            snapshot.shared_sequence_step_meta_elapsed_ms =
                Some(new_shared.query_result.step_meta_elapsed_ms);
            snapshot.shared_sequence_row_exists = Some(new_shared.query_result.row_exists);
            snapshot.shared_sequence_loads_connection_facts_before_meta_query =
                new_shared.loads_connection_facts_before_meta_query;
            snapshot.shared_sequence_uses_query_plus_next = new_shared.uses_query_plus_next;
            snapshot.shared_sequence_finalizes_exists_before_prepare_meta =
                new_shared.finalizes_exists_before_prepare_meta;
            snapshot.shared_sequence_extracts_phase_and_updated_at_after_step =
                new_shared.extracts_phase_and_updated_at_after_step;
            snapshot.shared_sequence_measures_prepare_meta_separately =
                new_shared.measures_prepare_meta_separately;
            snapshot.shared_sequence_measures_extract_separately =
                new_shared.measures_extract_separately;
            if tx.send(WorkerMessage::Snapshot(snapshot)).is_err() {
                return;
            }

            let _ = tx.send(WorkerMessage::Finished(Ok(())));
        });

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                diagnostic.budget_exhausted = true;
                diagnostic.total_elapsed_ms =
                    started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                return Ok(diagnostic);
            }

            match rx.recv_timeout(remaining) {
                Ok(WorkerMessage::Entered(stage)) => {
                    diagnostic.stage = stage;
                }
                Ok(WorkerMessage::Snapshot(snapshot)) => {
                    diagnostic.step_meta_detail_prepare_exists_elapsed_ms =
                        snapshot.step_meta_detail_prepare_exists_elapsed_ms;
                    diagnostic.step_meta_detail_step_exists_elapsed_ms =
                        snapshot.step_meta_detail_step_exists_elapsed_ms;
                    diagnostic.step_meta_detail_prepare_meta_elapsed_ms =
                        snapshot.step_meta_detail_prepare_meta_elapsed_ms;
                    diagnostic.step_meta_detail_step_meta_elapsed_ms =
                        snapshot.step_meta_detail_step_meta_elapsed_ms;
                    diagnostic.step_meta_detail_extract_phase_elapsed_ms =
                        snapshot.step_meta_detail_extract_phase_elapsed_ms;
                    diagnostic.step_meta_detail_extract_updated_at_elapsed_ms =
                        snapshot.step_meta_detail_extract_updated_at_elapsed_ms;
                    diagnostic.step_meta_detail_row_exists = snapshot.step_meta_detail_row_exists;
                    diagnostic.shared_sequence_prepare_exists_elapsed_ms =
                        snapshot.shared_sequence_prepare_exists_elapsed_ms;
                    diagnostic.shared_sequence_step_exists_elapsed_ms =
                        snapshot.shared_sequence_step_exists_elapsed_ms;
                    diagnostic.shared_sequence_step_meta_elapsed_ms =
                        snapshot.shared_sequence_step_meta_elapsed_ms;
                    diagnostic.shared_sequence_row_exists = snapshot.shared_sequence_row_exists;
                    diagnostic.step_meta_detail_loads_connection_facts_before_meta_query =
                        snapshot.step_meta_detail_loads_connection_facts_before_meta_query;
                    diagnostic.shared_sequence_loads_connection_facts_before_meta_query =
                        snapshot.shared_sequence_loads_connection_facts_before_meta_query;
                    diagnostic.step_meta_detail_uses_query_plus_next =
                        snapshot.step_meta_detail_uses_query_plus_next;
                    diagnostic.shared_sequence_uses_query_plus_next =
                        snapshot.shared_sequence_uses_query_plus_next;
                    diagnostic.step_meta_detail_finalizes_exists_before_prepare_meta =
                        snapshot.step_meta_detail_finalizes_exists_before_prepare_meta;
                    diagnostic.shared_sequence_finalizes_exists_before_prepare_meta =
                        snapshot.shared_sequence_finalizes_exists_before_prepare_meta;
                    diagnostic.step_meta_detail_extracts_phase_and_updated_at_after_step =
                        snapshot.step_meta_detail_extracts_phase_and_updated_at_after_step;
                    diagnostic.shared_sequence_extracts_phase_and_updated_at_after_step =
                        snapshot.shared_sequence_extracts_phase_and_updated_at_after_step;
                    diagnostic.step_meta_detail_measures_prepare_meta_separately =
                        snapshot.step_meta_detail_measures_prepare_meta_separately;
                    diagnostic.shared_sequence_measures_prepare_meta_separately =
                        snapshot.shared_sequence_measures_prepare_meta_separately;
                    diagnostic.step_meta_detail_measures_extract_separately =
                        snapshot.step_meta_detail_measures_extract_separately;
                    diagnostic.shared_sequence_measures_extract_separately =
                        snapshot.shared_sequence_measures_extract_separately;
                }
                Ok(WorkerMessage::Finished(result)) => {
                    if let Err(error) = result {
                        return Err(anyhow!(
                            "persisted rebuild row shared-path diff worker failed: {error}"
                        ));
                    }
                    diagnostic.stage = DiscoveryPersistedRebuildRowSharedPathDiffStage::Complete;
                    diagnostic.total_elapsed_ms =
                        started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    return Ok(diagnostic);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    diagnostic.budget_exhausted = true;
                    diagnostic.total_elapsed_ms =
                        started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    return Ok(diagnostic);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(anyhow!(
                        "persisted rebuild row shared-path diff worker disconnected before returning a result"
                    ));
                }
            }
        }
    }

    pub fn probe_discovery_persisted_rebuild_row_shared_sequence_compare_read_only(
        runtime_db_path: &Path,
        options: &DiscoveryPersistedRebuildRowSharedSequenceCompareOptions,
    ) -> Result<DiscoveryPersistedRebuildRowSharedSequenceCompareDiagnostic> {
        #[derive(Debug, Clone)]
        struct ProgressSnapshot {
            baseline_connection_facts: Option<crate::SqliteReadOnlyDriverCompareFacts>,
            baseline_with_exists_prepare_exists_elapsed_ms: Option<u64>,
            baseline_with_exists_step_exists_elapsed_ms: Option<u64>,
            baseline_with_exists_step_meta_elapsed_ms: Option<u64>,
            baseline_with_exists_row_exists: Option<bool>,
            shared_connection_no_exists_step_meta_elapsed_ms: Option<u64>,
            shared_connection_no_exists_row_exists: Option<bool>,
            prepare_exists_only_supported: bool,
            prepare_exists_only_prepare_exists_elapsed_ms: Option<u64>,
            prepare_exists_only_step_meta_elapsed_ms: Option<u64>,
            prepare_exists_only_row_exists: Option<bool>,
            explicit_reset_supported: bool,
            explicit_reset_kind: Option<String>,
            after_explicit_reset_prepare_exists_elapsed_ms: Option<u64>,
            after_explicit_reset_step_exists_elapsed_ms: Option<u64>,
            after_explicit_reset_reset_elapsed_ms: Option<u64>,
            after_explicit_reset_step_meta_elapsed_ms: Option<u64>,
            after_explicit_reset_row_exists: Option<bool>,
            fresh_connection_step_meta_elapsed_ms: Option<u64>,
            fresh_connection_row_exists: Option<bool>,
        }

        impl ProgressSnapshot {
            fn new(explicit_reset_supported: bool) -> Self {
                Self {
                    baseline_connection_facts: None,
                    baseline_with_exists_prepare_exists_elapsed_ms: None,
                    baseline_with_exists_step_exists_elapsed_ms: None,
                    baseline_with_exists_step_meta_elapsed_ms: None,
                    baseline_with_exists_row_exists: None,
                    shared_connection_no_exists_step_meta_elapsed_ms: None,
                    shared_connection_no_exists_row_exists: None,
                    prepare_exists_only_supported: true,
                    prepare_exists_only_prepare_exists_elapsed_ms: None,
                    prepare_exists_only_step_meta_elapsed_ms: None,
                    prepare_exists_only_row_exists: None,
                    explicit_reset_supported,
                    explicit_reset_kind: explicit_reset_supported
                        .then_some("pragma_shrink_memory".to_string()),
                    after_explicit_reset_prepare_exists_elapsed_ms: None,
                    after_explicit_reset_step_exists_elapsed_ms: None,
                    after_explicit_reset_reset_elapsed_ms: None,
                    after_explicit_reset_step_meta_elapsed_ms: None,
                    after_explicit_reset_row_exists: None,
                    fresh_connection_step_meta_elapsed_ms: None,
                    fresh_connection_row_exists: None,
                }
            }
        }

        #[derive(Debug)]
        enum WorkerMessage {
            Entered(DiscoveryPersistedRebuildRowSharedSequenceCompareStage),
            Snapshot(ProgressSnapshot),
            Finished(Result<(), String>),
        }

        #[derive(Debug)]
        struct QueryPlusNextVariantResult {
            step_meta_elapsed_ms: u64,
            row_exists: bool,
        }

        #[derive(Debug)]
        struct SharedSequenceVariantResult {
            prepare_exists_elapsed_ms: Option<u64>,
            step_exists_elapsed_ms: Option<u64>,
            reset_elapsed_ms: Option<u64>,
            query_result: QueryPlusNextVariantResult,
        }

        #[derive(Debug, Clone, Copy)]
        enum SharedSequencePrefixAction {
            None,
            PrepareExistsOnly,
            ExistsAndStepThenShrinkMemory,
        }

        fn skipped_stages(
            current_stage: DiscoveryPersistedRebuildRowSharedSequenceCompareStage,
            budget_exhausted: bool,
        ) -> Vec<DiscoveryPersistedRebuildRowSharedSequenceCompareStage> {
            if !budget_exhausted {
                return Vec::new();
            }
            let relevant = [
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::OpenBaselineSharedConnection,
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::BaselineWithExistsProbe,
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::SharedConnectionNoExistsPrefix,
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::SharedConnectionPrepareExistsOnlyPrefix,
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::SharedConnectionAfterExplicitReset,
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::FreshConnectionMetaOnly,
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::Complete,
            ];
            let cutoff_index = relevant
                .iter()
                .position(|stage| *stage == current_stage)
                .unwrap_or(relevant.len());
            relevant
                .into_iter()
                .skip(cutoff_index.saturating_add(1))
                .collect()
        }

        fn open_probe_store(runtime_db_path: &Path) -> Result<SqliteStore> {
            SqliteStore::open_read_only(runtime_db_path).with_context(|| {
                format!(
                    "failed opening runtime sqlite db read-only {}",
                    runtime_db_path.display()
                )
            })
        }

        fn run_query_plus_next_variant(
            store: &SqliteStore,
            force_step_delay_ms: Option<u64>,
        ) -> Result<QueryPlusNextVariantResult> {
            let mut meta_stmt = store
                .conn
                .prepare(
                    "SELECT phase, updated_at
                     FROM discovery_persisted_rebuild_state
                     WHERE id = 1",
                )
                .context("failed preparing persisted rebuild row meta query")?;
            let step_meta_started_at = Instant::now();
            if let Some(delay_ms) = force_step_delay_ms {
                std::thread::sleep(StdDuration::from_millis(delay_ms));
            }
            let mut meta_rows = meta_stmt
                .query([])
                .context("failed querying persisted rebuild row meta")?;
            let meta_row = meta_rows
                .next()
                .context("failed stepping persisted rebuild row meta")?;
            let step_meta_elapsed_ms = step_meta_started_at
                .elapsed()
                .as_millis()
                .min(u64::MAX as u128) as u64;
            let row_exists = meta_row.is_some();
            drop(meta_rows);
            drop(meta_stmt);
            Ok(QueryPlusNextVariantResult {
                step_meta_elapsed_ms,
                row_exists,
            })
        }

        fn run_shared_sequence_variant(
            runtime_db_path: &Path,
            prefix_action: SharedSequencePrefixAction,
            load_facts: bool,
            force_step_delay_ms: Option<u64>,
        ) -> Result<SharedSequenceVariantResult> {
            let store = open_probe_store(runtime_db_path)?;
            if load_facts {
                let _ = store.sqlite_read_only_driver_compare_facts()?;
            }

            let mut prepare_exists_elapsed_ms = None;
            let mut step_exists_elapsed_ms = None;
            let mut reset_elapsed_ms = None;

            match prefix_action {
                SharedSequencePrefixAction::None => {}
                SharedSequencePrefixAction::PrepareExistsOnly => {
                    let prepare_exists_started_at = Instant::now();
                    let exists_stmt = store
                        .conn
                        .prepare("SELECT 1 FROM discovery_persisted_rebuild_state WHERE id = 1")
                        .context("failed preparing shared-sequence exists probe")?;
                    prepare_exists_elapsed_ms = Some(
                        prepare_exists_started_at
                            .elapsed()
                            .as_millis()
                            .min(u64::MAX as u128) as u64,
                    );
                    drop(exists_stmt);
                }
                SharedSequencePrefixAction::ExistsAndStepThenShrinkMemory => {
                    let prepare_exists_started_at = Instant::now();
                    let mut exists_stmt = store
                        .conn
                        .prepare("SELECT 1 FROM discovery_persisted_rebuild_state WHERE id = 1")
                        .context("failed preparing shared-sequence exists probe")?;
                    prepare_exists_elapsed_ms = Some(
                        prepare_exists_started_at
                            .elapsed()
                            .as_millis()
                            .min(u64::MAX as u128) as u64,
                    );

                    let step_exists_started_at = Instant::now();
                    let exists_row = {
                        let mut exists_rows = exists_stmt
                            .query([])
                            .context("failed querying shared-sequence exists probe")?;
                        exists_rows
                            .next()
                            .context("failed stepping shared-sequence exists probe")?
                            .is_some()
                    };
                    step_exists_elapsed_ms = Some(
                        step_exists_started_at
                            .elapsed()
                            .as_millis()
                            .min(u64::MAX as u128) as u64,
                    );
                    drop(exists_stmt);

                    if !exists_row {
                        return Ok(SharedSequenceVariantResult {
                            prepare_exists_elapsed_ms,
                            step_exists_elapsed_ms,
                            reset_elapsed_ms,
                            query_result: QueryPlusNextVariantResult {
                                step_meta_elapsed_ms: 0,
                                row_exists: false,
                            },
                        });
                    }

                    if matches!(
                        prefix_action,
                        SharedSequencePrefixAction::ExistsAndStepThenShrinkMemory
                    ) {
                        let reset_started_at = Instant::now();
                        store
                            .conn
                            .execute_batch("PRAGMA shrink_memory;")
                            .context("failed applying PRAGMA shrink_memory on shared-sequence probe connection")?;
                        reset_elapsed_ms = Some(
                            reset_started_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
                        );
                    }
                }
            }

            let query_result = run_query_plus_next_variant(&store, force_step_delay_ms)?;
            Ok(SharedSequenceVariantResult {
                prepare_exists_elapsed_ms,
                step_exists_elapsed_ms,
                reset_elapsed_ms,
                query_result,
            })
        }

        let started_at = Instant::now();
        let deadline = started_at + StdDuration::from_millis(options.budget_ms);
        let mut diagnostic = DiscoveryPersistedRebuildRowSharedSequenceCompareDiagnostic {
            stage:
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::OpenBaselineSharedConnection,
            budget_exhausted: false,
            skipped_stages: Vec::new(),
            total_elapsed_ms: 0,
            baseline_connection_facts: None,
            baseline_with_exists_prepare_exists_elapsed_ms: None,
            baseline_with_exists_step_exists_elapsed_ms: None,
            baseline_with_exists_step_meta_elapsed_ms: None,
            baseline_with_exists_row_exists: None,
            shared_connection_no_exists_step_meta_elapsed_ms: None,
            shared_connection_no_exists_row_exists: None,
            prepare_exists_only_supported: true,
            prepare_exists_only_prepare_exists_elapsed_ms: None,
            prepare_exists_only_step_meta_elapsed_ms: None,
            prepare_exists_only_row_exists: None,
            explicit_reset_supported: !options.test_disable_explicit_reset_variant,
            explicit_reset_kind: (!options.test_disable_explicit_reset_variant)
                .then_some("pragma_shrink_memory".to_string()),
            after_explicit_reset_prepare_exists_elapsed_ms: None,
            after_explicit_reset_step_exists_elapsed_ms: None,
            after_explicit_reset_reset_elapsed_ms: None,
            after_explicit_reset_step_meta_elapsed_ms: None,
            after_explicit_reset_row_exists: None,
            fresh_connection_step_meta_elapsed_ms: None,
            fresh_connection_row_exists: None,
        };

        let (tx, rx) = mpsc::sync_channel(16);
        let runtime_db_path = runtime_db_path.to_path_buf();
        let options = options.clone();
        std::thread::spawn(move || {
            let send_finished = |result: Result<(), anyhow::Error>| {
                let _ = tx.send(WorkerMessage::Finished(
                    result.map_err(|error| format!("{error:#}")),
                ));
            };

            let mut snapshot = ProgressSnapshot::new(!options.test_disable_explicit_reset_variant);

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::OpenBaselineSharedConnection,
            ));
            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::BaselineWithExistsProbe,
            ));
            let baseline = match Self::run_isolated_shared_sequence_baseline_path(
                &runtime_db_path,
                options.test_force_baseline_with_exists_step_meta_delay_ms,
            ) {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.baseline_connection_facts = Some(baseline.connection_facts);
            snapshot.baseline_with_exists_prepare_exists_elapsed_ms =
                Some(baseline.exists_probe.prepare_exists_elapsed_ms);
            snapshot.baseline_with_exists_step_exists_elapsed_ms =
                Some(baseline.exists_probe.step_exists_elapsed_ms);
            snapshot.baseline_with_exists_step_meta_elapsed_ms =
                Some(baseline.query_result.step_meta_elapsed_ms);
            snapshot.baseline_with_exists_row_exists = Some(baseline.query_result.row_exists);
            if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                return;
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::SharedConnectionNoExistsPrefix,
            ));
            let no_exists = match run_shared_sequence_variant(
                &runtime_db_path,
                SharedSequencePrefixAction::None,
                true,
                options.test_force_shared_no_exists_step_meta_delay_ms,
            ) {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.shared_connection_no_exists_step_meta_elapsed_ms =
                Some(no_exists.query_result.step_meta_elapsed_ms);
            snapshot.shared_connection_no_exists_row_exists =
                Some(no_exists.query_result.row_exists);
            if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                return;
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::SharedConnectionPrepareExistsOnlyPrefix,
            ));
            let prepare_only = match run_shared_sequence_variant(
                &runtime_db_path,
                SharedSequencePrefixAction::PrepareExistsOnly,
                true,
                options.test_force_prepare_exists_only_step_meta_delay_ms,
            ) {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.prepare_exists_only_prepare_exists_elapsed_ms =
                prepare_only.prepare_exists_elapsed_ms;
            snapshot.prepare_exists_only_step_meta_elapsed_ms =
                Some(prepare_only.query_result.step_meta_elapsed_ms);
            snapshot.prepare_exists_only_row_exists = Some(prepare_only.query_result.row_exists);
            if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                return;
            }

            if !options.test_disable_explicit_reset_variant {
                let _ = tx.send(WorkerMessage::Entered(
                    DiscoveryPersistedRebuildRowSharedSequenceCompareStage::SharedConnectionAfterExplicitReset,
                ));
                let after_reset = match run_shared_sequence_variant(
                    &runtime_db_path,
                    SharedSequencePrefixAction::ExistsAndStepThenShrinkMemory,
                    true,
                    options.test_force_after_explicit_reset_step_meta_delay_ms,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        send_finished(Err(error));
                        return;
                    }
                };
                snapshot.after_explicit_reset_prepare_exists_elapsed_ms =
                    after_reset.prepare_exists_elapsed_ms;
                snapshot.after_explicit_reset_step_exists_elapsed_ms =
                    after_reset.step_exists_elapsed_ms;
                snapshot.after_explicit_reset_reset_elapsed_ms = after_reset.reset_elapsed_ms;
                snapshot.after_explicit_reset_step_meta_elapsed_ms =
                    Some(after_reset.query_result.step_meta_elapsed_ms);
                snapshot.after_explicit_reset_row_exists =
                    Some(after_reset.query_result.row_exists);
                if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                    return;
                }
            } else {
                snapshot.explicit_reset_supported = false;
                snapshot.explicit_reset_kind = None;
                if tx.send(WorkerMessage::Snapshot(snapshot.clone())).is_err() {
                    return;
                }
            }

            let _ = tx.send(WorkerMessage::Entered(
                DiscoveryPersistedRebuildRowSharedSequenceCompareStage::FreshConnectionMetaOnly,
            ));
            let fresh = match run_shared_sequence_variant(
                &runtime_db_path,
                SharedSequencePrefixAction::None,
                false,
                options.test_force_fresh_connection_step_meta_delay_ms,
            ) {
                Ok(value) => value,
                Err(error) => {
                    send_finished(Err(error));
                    return;
                }
            };
            snapshot.fresh_connection_step_meta_elapsed_ms =
                Some(fresh.query_result.step_meta_elapsed_ms);
            snapshot.fresh_connection_row_exists = Some(fresh.query_result.row_exists);
            if tx.send(WorkerMessage::Snapshot(snapshot)).is_err() {
                return;
            }

            let _ = tx.send(WorkerMessage::Finished(Ok(())));
        });

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                diagnostic.budget_exhausted = true;
                diagnostic.total_elapsed_ms =
                    started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                diagnostic.skipped_stages = skipped_stages(diagnostic.stage, true);
                return Ok(diagnostic);
            }

            match rx.recv_timeout(remaining) {
                Ok(WorkerMessage::Entered(stage)) => {
                    diagnostic.stage = stage;
                }
                Ok(WorkerMessage::Snapshot(snapshot)) => {
                    diagnostic.baseline_connection_facts = snapshot.baseline_connection_facts;
                    diagnostic.baseline_with_exists_prepare_exists_elapsed_ms =
                        snapshot.baseline_with_exists_prepare_exists_elapsed_ms;
                    diagnostic.baseline_with_exists_step_exists_elapsed_ms =
                        snapshot.baseline_with_exists_step_exists_elapsed_ms;
                    diagnostic.baseline_with_exists_step_meta_elapsed_ms =
                        snapshot.baseline_with_exists_step_meta_elapsed_ms;
                    diagnostic.baseline_with_exists_row_exists =
                        snapshot.baseline_with_exists_row_exists;
                    diagnostic.shared_connection_no_exists_step_meta_elapsed_ms =
                        snapshot.shared_connection_no_exists_step_meta_elapsed_ms;
                    diagnostic.shared_connection_no_exists_row_exists =
                        snapshot.shared_connection_no_exists_row_exists;
                    diagnostic.prepare_exists_only_supported =
                        snapshot.prepare_exists_only_supported;
                    diagnostic.prepare_exists_only_prepare_exists_elapsed_ms =
                        snapshot.prepare_exists_only_prepare_exists_elapsed_ms;
                    diagnostic.prepare_exists_only_step_meta_elapsed_ms =
                        snapshot.prepare_exists_only_step_meta_elapsed_ms;
                    diagnostic.prepare_exists_only_row_exists =
                        snapshot.prepare_exists_only_row_exists;
                    diagnostic.explicit_reset_supported = snapshot.explicit_reset_supported;
                    diagnostic.explicit_reset_kind = snapshot.explicit_reset_kind;
                    diagnostic.after_explicit_reset_prepare_exists_elapsed_ms =
                        snapshot.after_explicit_reset_prepare_exists_elapsed_ms;
                    diagnostic.after_explicit_reset_step_exists_elapsed_ms =
                        snapshot.after_explicit_reset_step_exists_elapsed_ms;
                    diagnostic.after_explicit_reset_reset_elapsed_ms =
                        snapshot.after_explicit_reset_reset_elapsed_ms;
                    diagnostic.after_explicit_reset_step_meta_elapsed_ms =
                        snapshot.after_explicit_reset_step_meta_elapsed_ms;
                    diagnostic.after_explicit_reset_row_exists =
                        snapshot.after_explicit_reset_row_exists;
                    diagnostic.fresh_connection_step_meta_elapsed_ms =
                        snapshot.fresh_connection_step_meta_elapsed_ms;
                    diagnostic.fresh_connection_row_exists = snapshot.fresh_connection_row_exists;
                }
                Ok(WorkerMessage::Finished(result)) => {
                    if let Err(error) = result {
                        return Err(anyhow!(
                            "persisted rebuild row shared-sequence compare worker failed: {error}"
                        ));
                    }
                    diagnostic.stage =
                        DiscoveryPersistedRebuildRowSharedSequenceCompareStage::Complete;
                    diagnostic.total_elapsed_ms =
                        started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    diagnostic.skipped_stages = Vec::new();
                    return Ok(diagnostic);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    diagnostic.budget_exhausted = true;
                    diagnostic.total_elapsed_ms =
                        started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    diagnostic.skipped_stages = skipped_stages(diagnostic.stage, true);
                    return Ok(diagnostic);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(anyhow!(
                        "persisted rebuild row shared-sequence compare worker disconnected before returning a result"
                    ));
                }
            }
        }
    }
    pub fn load_discovery_persisted_rebuild_state_json_bytes_read_only(
        &self,
    ) -> Result<Option<usize>> {
        if !self.sqlite_table_exists("discovery_persisted_rebuild_state")? {
            return Ok(None);
        }
        self.load_discovery_persisted_rebuild_state_json_bytes_after_table_exists_read_only()
    }

    pub fn load_discovery_persisted_rebuild_state_meta_read_only(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateMetaRow>> {
        let Some(raw) = self.load_discovery_persisted_rebuild_state_meta_lite_raw_read_only()?
        else {
            return Ok(None);
        };
        Ok(Some(DiscoveryPersistedRebuildStateMetaRow {
            phase: DiscoveryPersistedRebuildPhase::parse(&raw.phase_raw)?,
            state_json_bytes: self
                .load_discovery_persisted_rebuild_state_json_bytes_read_only()?
                .unwrap_or(0),
            updated_at: parse_rfc3339_utc(
                &raw.updated_at_raw,
                "discovery_persisted_rebuild_state.updated_at",
            )?,
        }))
    }

    fn load_discovery_persisted_rebuild_state_query(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateRow>> {
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

fn recent_raw_journal_write_summary(
    state: &RecentRawJournalStateRow,
    batch_rows: usize,
    inserted_rows: usize,
) -> RecentRawJournalWriteSummary {
    RecentRawJournalWriteSummary {
        batch_rows,
        inserted_rows,
        covered_since: state.covered_since,
        covered_through_cursor: state.covered_through_cursor.clone(),
        row_count: state.row_count,
        last_batch_completed_at: state.last_batch_completed_at,
        ..RecentRawJournalWriteSummary::default()
    }
}

fn advance_recent_raw_journal_state_for_batch(
    state: &mut RecentRawJournalStateRow,
    processed_swaps: &[SwapEvent],
    inserted_rows: usize,
    completed_at: DateTime<Utc>,
) {
    if processed_swaps.is_empty() {
        return;
    }
    if inserted_rows > 0 {
        if let Some(first_swap) = processed_swaps.first() {
            state.covered_since = Some(match state.covered_since {
                Some(existing) if existing <= first_swap.ts_utc => existing,
                _ => first_swap.ts_utc,
            });
        }
        state.row_count = state.row_count.saturating_add(inserted_rows);
    }
    if let Some(last_swap) = processed_swaps.last() {
        let last_cursor = DiscoveryRuntimeCursor {
            ts_utc: last_swap.ts_utc,
            slot: last_swap.slot,
            signature: last_swap.signature.clone(),
        };
        let should_advance_cursor = state
            .covered_through_cursor
            .as_ref()
            .map(|cursor| discovery_runtime_cursor_cmp(&last_cursor, cursor).is_gt())
            .unwrap_or(state.row_count > 0 || inserted_rows > 0);
        if should_advance_cursor {
            state.covered_through_cursor = Some(last_cursor);
        }
    }
    state.last_batch_rows = inserted_rows;
    state.last_batch_completed_at = Some(completed_at);
    state.updated_at = Some(completed_at);
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration as StdDuration, Instant};
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

    fn seed_wallet_activity_interrupt_fixture(
        store: &mut SqliteStore,
        since: DateTime<Utc>,
        wallet_count: usize,
        prefix: &str,
    ) -> Result<Vec<String>> {
        let mut swaps = Vec::with_capacity(wallet_count);
        let mut wallet_ids = Vec::with_capacity(wallet_count);
        for idx in 0..wallet_count {
            let wallet_id = format!("{prefix}-wallet-{idx:05}");
            wallet_ids.push(wallet_id.clone());
            swaps.push(swap(
                &format!("{prefix}-sig-{idx:05}"),
                &wallet_id,
                since + Duration::seconds((idx % 3_600) as i64),
                SOL_MINT,
                &format!("{prefix}-token-{idx:05}"),
                idx as u64 + 1,
            ));
        }
        store.insert_observed_swaps_batch_with_activity_days(&swaps)?;
        Ok(wallet_ids)
    }

    fn spawn_interrupt_loop(
        interrupt_handle: rusqlite::InterruptHandle,
        warmup: StdDuration,
    ) -> (Arc<AtomicBool>, thread::JoinHandle<()>) {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let join = thread::spawn(move || {
            if warmup > StdDuration::from_millis(0) {
                thread::sleep(warmup);
            }
            while !stop_flag.load(Ordering::Relaxed) {
                interrupt_handle.interrupt();
                thread::sleep(StdDuration::from_micros(50));
            }
        });
        (stop, join)
    }

    #[derive(Debug, Clone, Copy)]
    struct CheckpointRecurrenceSummary {
        writes_before_reader: usize,
        writes_during_reader: usize,
        max_backlog_frames: i64,
    }

    #[derive(Debug, Clone, Copy)]
    enum CursorCheckpointRecurrenceReader {
        LegacyAfterCursorSingleStatement,
        ChunkedAfterCursor,
        LegacySolLegSingleStatement,
        ChunkedSolLeg,
    }

    impl CursorCheckpointRecurrenceReader {
        fn db_name(self) -> &'static str {
            match self {
                Self::LegacyAfterCursorSingleStatement => {
                    "observed-swap-after-cursor-single-statement-recurrence.db"
                }
                Self::ChunkedAfterCursor => "observed-swap-after-cursor-chunked-recurrence.db",
                Self::LegacySolLegSingleStatement => {
                    "observed-sol-leg-single-statement-recurrence.db"
                }
                Self::ChunkedSolLeg => "observed-sol-leg-chunked-recurrence.db",
            }
        }
    }

    fn run_checkpoint_recurrence_scenario(
        use_paged_reader: bool,
    ) -> Result<CheckpointRecurrenceSummary> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join(if use_paged_reader {
            "observed-swap-window-paged-recurrence.db"
        } else {
            "observed-swap-window-unpaged-recurrence.db"
        });
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-04-07T18:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let reader_window_start = now - Duration::days(2);
        let reader_window_end = now - Duration::hours(1);
        let mut seed_rows = Vec::new();
        for idx in 0..512usize {
            seed_rows.push(swap(
                &format!("sig-checkpoint-seed-{idx:04}"),
                &format!("wallet-seed-{:03}", idx % 24),
                reader_window_start + Duration::seconds(idx as i64),
                SOL_MINT,
                &format!("TokenCheckpointSeed{idx:04}"),
                10_000 + idx as u64,
            ));
        }
        seed_store.insert_observed_swaps_batch_with_activity_days(&seed_rows)?;
        seed_store.checkpoint_wal_truncate()?;

        let writes_completed = Arc::new(AtomicUsize::new(0));
        let stop_writes = Arc::new(AtomicBool::new(false));
        let writer_db_path = db_path.clone();
        let writer_writes_completed = Arc::clone(&writes_completed);
        let writer_stop = Arc::clone(&stop_writes);
        let writer = thread::spawn(move || -> Result<()> {
            let writer_store = SqliteStore::open(Path::new(&writer_db_path))?;
            writer_store
                .conn
                .pragma_update(None, "wal_autocheckpoint", 1_i64)
                .context(
                    "failed to force aggressive wal_autocheckpoint for recurrence test writer",
                )?;
            let mut counter = 0usize;
            while !writer_stop.load(Ordering::Relaxed) {
                let swap = swap(
                    &format!("sig-checkpoint-live-{counter:06}"),
                    &format!("wallet-live-{:03}", counter % 32),
                    now + Duration::milliseconds(counter as i64),
                    SOL_MINT,
                    &format!("TokenCheckpointLive{:06}", counter % 64),
                    20_000 + counter as u64,
                );
                writer_store.insert_observed_swaps_batch_with_activity_days(&[swap])?;
                writer_writes_completed.fetch_add(1, Ordering::Relaxed);
                counter = counter.saturating_add(1);
            }
            Ok(())
        });

        let baseline_started = Instant::now();
        while writes_completed.load(Ordering::Relaxed) < 32 {
            if baseline_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("writer failed to establish post-checkpoint baseline throughput");
            }
            thread::sleep(StdDuration::from_millis(10));
        }
        let writes_before_reader = writes_completed.load(Ordering::Relaxed);

        let reader_started = Arc::new(AtomicBool::new(false));
        let reader_db_path = db_path.clone();
        let reader_started_flag = Arc::clone(&reader_started);
        let reader = thread::spawn(move || -> Result<()> {
            let reader_store = SqliteStore::open_read_only(Path::new(&reader_db_path))?;
            if use_paged_reader {
                reader_store.for_each_observed_swap_in_window_paged(
                    reader_window_start,
                    reader_window_end,
                    32,
                    |swap| {
                        if !reader_started_flag.swap(true, Ordering::Relaxed) {
                            let _ = swap.signature.as_str();
                        }
                        thread::sleep(StdDuration::from_millis(1));
                        Ok(())
                    },
                )?;
            } else {
                reader_store.for_each_observed_swap_in_window(
                    reader_window_start,
                    reader_window_end,
                    |swap| {
                        if !reader_started_flag.swap(true, Ordering::Relaxed) {
                            let _ = swap.signature.as_str();
                        }
                        thread::sleep(StdDuration::from_millis(1));
                        Ok(())
                    },
                )?;
            }
            Ok(())
        });

        let reader_started_wait = Instant::now();
        while !reader_started.load(Ordering::Relaxed) {
            if reader_started_wait.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("reader failed to start recurrence scenario");
            }
            thread::sleep(StdDuration::from_millis(5));
        }

        let monitor_store = SqliteStore::open(Path::new(&db_path))?;
        let mut max_backlog_frames = 0i64;
        while !reader.is_finished() {
            let (_, log_frames, checkpointed_frames) = monitor_store.checkpoint_wal_passive()?;
            max_backlog_frames =
                max_backlog_frames.max(log_frames.saturating_sub(checkpointed_frames));
            thread::sleep(StdDuration::from_millis(10));
        }
        reader
            .join()
            .expect("reader thread panicked")
            .context("reader recurrence scenario failed")?;

        stop_writes.store(true, Ordering::Relaxed);
        writer
            .join()
            .expect("writer thread panicked")
            .context("writer recurrence scenario failed")?;

        let writes_during_reader = writes_completed
            .load(Ordering::Relaxed)
            .saturating_sub(writes_before_reader);
        monitor_store.checkpoint_wal_truncate()?;

        Ok(CheckpointRecurrenceSummary {
            writes_before_reader,
            writes_during_reader,
            max_backlog_frames,
        })
    }

    fn run_cursor_checkpoint_recurrence_scenario(
        reader_mode: CursorCheckpointRecurrenceReader,
    ) -> Result<CheckpointRecurrenceSummary> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join(reader_mode.db_name());
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-04-09T18:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let reader_window_start = now - Duration::days(2);
        let reader_window_end = now - Duration::hours(1);
        let mut seed_rows = Vec::new();
        for idx in 0..8_192usize {
            let token_in = if idx % 2 == 0 {
                SOL_MINT.to_string()
            } else {
                format!("TokenCursorSeedIn{idx:05}")
            };
            let token_out = if idx % 2 == 0 {
                format!("TokenCursorSeedOut{idx:05}")
            } else {
                SOL_MINT.to_string()
            };
            seed_rows.push(swap(
                &format!("sig-cursor-checkpoint-seed-{idx:05}"),
                &format!("wallet-cursor-seed-{:03}", idx % 96),
                reader_window_start + Duration::seconds(idx as i64),
                &token_in,
                &token_out,
                30_000 + idx as u64,
            ));
        }
        seed_store.insert_observed_swaps_batch_with_activity_days(&seed_rows)?;
        seed_store.checkpoint_wal_truncate()?;

        let writes_completed = Arc::new(AtomicUsize::new(0));
        let stop_writes = Arc::new(AtomicBool::new(false));
        let writer_db_path = db_path.clone();
        let writer_writes_completed = Arc::clone(&writes_completed);
        let writer_stop = Arc::clone(&stop_writes);
        let writer = thread::spawn(move || -> Result<()> {
            let writer_store = SqliteStore::open(Path::new(&writer_db_path))?;
            writer_store
                .conn
                .pragma_update(None, "wal_autocheckpoint", 1_i64)
                .context(
                    "failed to force aggressive wal_autocheckpoint for cursor recurrence test writer",
                )?;
            let mut counter = 0usize;
            while !writer_stop.load(Ordering::Relaxed) {
                let live_swap = swap(
                    &format!("sig-cursor-checkpoint-live-{counter:06}"),
                    &format!("wallet-cursor-live-{:03}", counter % 64),
                    now + Duration::milliseconds(counter as i64),
                    SOL_MINT,
                    &format!("TokenCursorLive{:06}", counter % 128),
                    50_000 + counter as u64,
                );
                writer_store.insert_observed_swaps_batch_with_activity_days(&[live_swap])?;
                writer_writes_completed.fetch_add(1, Ordering::Relaxed);
                counter = counter.saturating_add(1);
            }
            Ok(())
        });

        let baseline_started = Instant::now();
        while writes_completed.load(Ordering::Relaxed) < 32 {
            if baseline_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "writer failed to establish post-checkpoint baseline throughput for cursor recurrence scenario"
                );
            }
            thread::sleep(StdDuration::from_millis(10));
        }
        let writes_before_reader = writes_completed.load(Ordering::Relaxed);

        let reader_started = Arc::new(AtomicBool::new(false));
        let reader_db_path = db_path.clone();
        let reader_started_flag = Arc::clone(&reader_started);
        let reader = thread::spawn(move || -> Result<()> {
            let reader_store = SqliteStore::open_read_only(Path::new(&reader_db_path))?;
            let reader_deadline = Instant::now() + StdDuration::from_secs(30);
            match reader_mode {
                CursorCheckpointRecurrenceReader::LegacyAfterCursorSingleStatement => {
                    reader_store.for_each_observed_swap_after_cursor_single_statement_with_budget(
                        reader_window_start - Duration::seconds(1),
                        0,
                        "",
                        4_096,
                        reader_deadline,
                        |swap| {
                            if !reader_started_flag.swap(true, Ordering::Relaxed) {
                                let _ = swap.signature.as_str();
                            }
                            thread::sleep(StdDuration::from_millis(1));
                            Ok(())
                        },
                    )?;
                }
                CursorCheckpointRecurrenceReader::ChunkedAfterCursor => {
                    reader_store.for_each_observed_swap_after_cursor_with_budget(
                        reader_window_start - Duration::seconds(1),
                        0,
                        "",
                        4_096,
                        reader_deadline,
                        |swap| {
                            if !reader_started_flag.swap(true, Ordering::Relaxed) {
                                let _ = swap.signature.as_str();
                            }
                            thread::sleep(StdDuration::from_millis(1));
                            Ok(())
                        },
                    )?;
                }
                CursorCheckpointRecurrenceReader::LegacySolLegSingleStatement => {
                    reader_store
                        .for_each_observed_sol_leg_swap_in_window_after_cursor_single_statement_with_budget(
                            reader_window_start,
                            reader_window_end,
                            None,
                            4_096,
                            reader_deadline,
                            |swap| {
                                if !reader_started_flag.swap(true, Ordering::Relaxed) {
                                    let _ = swap.signature.as_str();
                                }
                                thread::sleep(StdDuration::from_millis(1));
                                Ok(())
                            },
                        )?;
                }
                CursorCheckpointRecurrenceReader::ChunkedSolLeg => {
                    reader_store
                        .for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget(
                            reader_window_start,
                            reader_window_end,
                            None,
                            4_096,
                            reader_deadline,
                            |swap| {
                                if !reader_started_flag.swap(true, Ordering::Relaxed) {
                                    let _ = swap.signature.as_str();
                                }
                                thread::sleep(StdDuration::from_millis(1));
                                Ok(())
                            },
                        )?;
                }
            }
            Ok(())
        });

        let reader_started_wait = Instant::now();
        while !reader_started.load(Ordering::Relaxed) {
            if reader_started_wait.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("reader failed to start cursor recurrence scenario");
            }
            thread::sleep(StdDuration::from_millis(5));
        }

        let monitor_store = SqliteStore::open(Path::new(&db_path))?;
        let mut max_backlog_frames = 0i64;
        while !reader.is_finished() {
            let (_, log_frames, checkpointed_frames) = monitor_store.checkpoint_wal_passive()?;
            max_backlog_frames =
                max_backlog_frames.max(log_frames.saturating_sub(checkpointed_frames));
            thread::sleep(StdDuration::from_millis(10));
        }
        reader
            .join()
            .expect("reader thread panicked")
            .context("cursor recurrence scenario reader failed")?;

        stop_writes.store(true, Ordering::Relaxed);
        writer
            .join()
            .expect("writer thread panicked")
            .context("cursor recurrence scenario writer failed")?;

        let writes_during_reader = writes_completed
            .load(Ordering::Relaxed)
            .saturating_sub(writes_before_reader);
        monitor_store.checkpoint_wal_truncate()?;

        Ok(CheckpointRecurrenceSummary {
            writes_before_reader,
            writes_during_reader,
            max_backlog_frames,
        })
    }

    #[test]
    fn observed_swap_window_paged_reader_matches_unpaged_stream_results_stage1() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("observed-swap-window-paged-equivalence.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;
        let now = DateTime::parse_from_rfc3339("2026-04-07T18:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let since = now - Duration::hours(2);
        let until = now;
        let swaps = (0..257usize)
            .map(|idx| {
                swap(
                    &format!("sig-window-paged-equivalence-{idx:04}"),
                    &format!("wallet-window-{:03}", idx % 7),
                    since + Duration::seconds(idx as i64),
                    SOL_MINT,
                    &format!("TokenWindowEquivalence{idx:04}"),
                    40_000 + idx as u64,
                )
            })
            .collect::<Vec<_>>();
        store.insert_observed_swaps_batch_with_activity_days(&swaps)?;

        let mut unpaged_signatures = Vec::new();
        store.for_each_observed_swap_in_window(since, until, |swap| {
            unpaged_signatures.push(swap.signature);
            Ok(())
        })?;

        let mut paged_signatures = Vec::new();
        let summary = store.for_each_observed_swap_in_window_paged_with_budget(
            since,
            until,
            32,
            Instant::now() + StdDuration::from_secs(5),
            |swap| {
                paged_signatures.push(swap.signature);
                Ok(())
            },
        )?;

        assert_eq!(summary.rows_seen, unpaged_signatures.len());
        assert!(!summary.time_budget_exhausted);
        assert_eq!(paged_signatures, unpaged_signatures);
        Ok(())
    }

    #[test]
    fn observed_swap_window_paged_reader_prevents_post_checkpoint_recurrence_stage1() -> Result<()>
    {
        let unpaged = run_checkpoint_recurrence_scenario(false)?;
        let paged = run_checkpoint_recurrence_scenario(true)?;

        assert!(
            unpaged.writes_before_reader >= 32,
            "clean checkpoint should permit an immediate post-start write baseline before the long reader begins: {unpaged:?}"
        );
        assert!(
            paged.writes_before_reader >= 32,
            "paged scenario should also establish the same clean post-checkpoint baseline: {paged:?}"
        );
        assert!(
            unpaged.max_backlog_frames >= paged.max_backlog_frames.saturating_mul(2),
            "the current single-statement reader should strand materially more WAL frames behind the oldest reader mark than the paged reader: unpaged={unpaged:?} paged={paged:?}"
        );
        assert!(
            unpaged.max_backlog_frames.saturating_sub(paged.max_backlog_frames) >= 5_000,
            "the long reader should create a materially larger checkpoint debt even when scheduler jitter makes raw write counts noisy: unpaged={unpaged:?} paged={paged:?}"
        );
        assert!(
            unpaged.writes_during_reader > 0 && paged.writes_during_reader > 0,
            "both scenarios should continue writing after the clean checkpoint baseline so the recurrence is exercised under active writer load: unpaged={unpaged:?} paged={paged:?}"
        );
        Ok(())
    }

    #[test]
    fn observed_swap_after_cursor_chunked_reader_prevents_post_checkpoint_recurrence_stage1(
    ) -> Result<()> {
        let legacy = run_cursor_checkpoint_recurrence_scenario(
            CursorCheckpointRecurrenceReader::LegacyAfterCursorSingleStatement,
        )?;
        let chunked = run_cursor_checkpoint_recurrence_scenario(
            CursorCheckpointRecurrenceReader::ChunkedAfterCursor,
        )?;

        assert!(
            legacy.writes_before_reader >= 32 && chunked.writes_before_reader >= 32,
            "both cursor scenarios should establish the same clean post-checkpoint write baseline before the long reader begins: legacy={legacy:?} chunked={chunked:?}"
        );
        assert!(
            legacy.max_backlog_frames
                >= chunked
                    .max_backlog_frames
                    .saturating_mul(15)
                    .saturating_add(9)
                    / 10,
            "the legacy single-statement after-cursor reader should strand materially more WAL frames than the chunked production reader: legacy={legacy:?} chunked={chunked:?}"
        );
        assert!(
            legacy.max_backlog_frames.saturating_sub(chunked.max_backlog_frames) >= 250_000,
            "the chunked after-cursor reader should materially reduce checkpoint debt on the same active-writer workload: legacy={legacy:?} chunked={chunked:?}"
        );
        assert!(
            legacy.writes_during_reader > 0 && chunked.writes_during_reader > 0,
            "both cursor scenarios should continue writing after the clean checkpoint baseline so the recurrence is exercised under active writer load: legacy={legacy:?} chunked={chunked:?}"
        );
        Ok(())
    }

    #[test]
    fn observed_sol_leg_cursor_chunked_reader_prevents_post_checkpoint_recurrence_stage1(
    ) -> Result<()> {
        let legacy = run_cursor_checkpoint_recurrence_scenario(
            CursorCheckpointRecurrenceReader::LegacySolLegSingleStatement,
        )?;
        let chunked = run_cursor_checkpoint_recurrence_scenario(
            CursorCheckpointRecurrenceReader::ChunkedSolLeg,
        )?;

        assert!(
            legacy.writes_before_reader >= 32 && chunked.writes_before_reader >= 32,
            "both SOL-leg cursor scenarios should establish the same clean post-checkpoint write baseline before the long reader begins: legacy={legacy:?} chunked={chunked:?}"
        );
        assert!(
            legacy.max_backlog_frames
                >= chunked
                    .max_backlog_frames
                    .saturating_mul(15)
                    .saturating_add(9)
                    / 10,
            "the legacy single-statement SOL-leg cursor reader should strand materially more WAL frames than the chunked production reader: legacy={legacy:?} chunked={chunked:?}"
        );
        assert!(
            legacy.max_backlog_frames.saturating_sub(chunked.max_backlog_frames) >= 250_000,
            "the chunked SOL-leg reader should materially reduce checkpoint debt on the same active-writer workload: legacy={legacy:?} chunked={chunked:?}"
        );
        assert!(
            legacy.writes_during_reader > 0 && chunked.writes_during_reader > 0,
            "both SOL-leg scenarios should continue writing after the clean checkpoint baseline so the recurrence is exercised under active writer load: legacy={legacy:?} chunked={chunked:?}"
        );
        Ok(())
    }

    #[test]
    fn observed_swap_window_after_cursor_chunked_reader_preserves_resume_order_stage1() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("observed-swap-window-after-cursor-resume-order.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let base = DateTime::parse_from_rfc3339("2026-04-09T12:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        for (idx, signature) in ["sig-a", "sig-b", "sig-c", "sig-d"].into_iter().enumerate() {
            assert!(store.insert_observed_swap(&SwapEvent {
                signature: signature.to_string(),
                wallet: format!("wallet-window-cursor-{idx:02}"),
                dex: "raydium".to_string(),
                token_in: SOL_MINT.to_string(),
                token_out: format!("TokenWindowCursor{idx:02}"),
                amount_in: 1.0,
                amount_out: 10.0 + idx as f64,
                slot: 10 + idx as u64,
                ts_utc: base + Duration::seconds(idx as i64),
                exact_amounts: None,
            })?);
        }

        let mut first_page = Vec::new();
        let first = store.for_each_observed_swap_in_window_after_cursor_with_budget(
            base,
            base + Duration::seconds(10),
            None,
            2,
            Instant::now() + StdDuration::from_secs(1),
            |swap| {
                first_page.push(swap.signature);
                Ok(())
            },
        )?;
        assert_eq!(first.rows_seen, 2);
        assert!(!first.time_budget_exhausted);
        assert_eq!(first_page, vec!["sig-a".to_string(), "sig-b".to_string()]);

        let cursor = DiscoveryRuntimeCursor {
            ts_utc: base + Duration::seconds(1),
            slot: 11,
            signature: "sig-b".to_string(),
        };
        let mut second_page = Vec::new();
        let second = store.for_each_observed_swap_in_window_after_cursor_with_budget(
            base,
            base + Duration::seconds(10),
            Some(&cursor),
            2,
            Instant::now() + StdDuration::from_secs(1),
            |swap| {
                second_page.push(swap.signature);
                Ok(())
            },
        )?;
        assert_eq!(second.rows_seen, 2);
        assert!(!second.time_budget_exhausted);
        assert_eq!(second_page, vec!["sig-c".to_string(), "sig-d".to_string()]);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_effective_bulk_insert_chunk_rows_honors_sqlite_variable_limit() {
        assert_eq!(
            recent_raw_journal_effective_bulk_insert_chunk_rows(None, 999),
            76
        );
        assert_eq!(
            recent_raw_journal_effective_bulk_insert_chunk_rows(Some(512), 999),
            76
        );
    }

    #[test]
    fn recent_raw_journal_effective_bulk_insert_chunk_rows_uses_hard_cap_for_high_limit() {
        assert_eq!(
            recent_raw_journal_effective_bulk_insert_chunk_rows(None, 32_766),
            RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS
        );
        assert_eq!(
            recent_raw_journal_effective_bulk_insert_chunk_rows(Some(4096), 32_766),
            RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS
        );
    }

    #[test]
    fn recent_raw_journal_effective_bulk_insert_chunk_rows_preserves_forced_small_test_chunk() {
        assert_eq!(
            recent_raw_journal_effective_bulk_insert_chunk_rows(Some(16), 32_766),
            16
        );
        assert_eq!(
            recent_raw_journal_effective_bulk_insert_chunk_rows(Some(0), 32_766),
            1
        );
    }

    #[test]
    fn recent_raw_journal_batch_write_keeps_cached_state_exact() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("recent-raw-cached-state.db");
        let store = SqliteStore::open(Path::new(&db_path))?;
        let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let first_batch = vec![
            swap(
                "sig-recent-raw-state-a",
                "wallet-a",
                now - Duration::minutes(2),
                SOL_MINT,
                "TokenRecentRawStateA111111111111111111111111",
                100,
            ),
            swap(
                "sig-recent-raw-state-b",
                "wallet-a",
                now - Duration::minutes(1),
                SOL_MINT,
                "TokenRecentRawStateB111111111111111111111111",
                101,
            ),
        ];
        let first_summary = store.insert_recent_raw_journal_batch(&first_batch, now)?;
        assert_eq!(first_summary.batch_rows, 2);
        assert_eq!(first_summary.inserted_rows, 2);

        let second_batch = vec![
            first_batch[1].clone(),
            swap(
                "sig-recent-raw-state-c",
                "wallet-a",
                now,
                SOL_MINT,
                "TokenRecentRawStateC111111111111111111111111",
                102,
            ),
        ];
        let second_summary = store.insert_recent_raw_journal_batch(&second_batch, now)?;
        assert_eq!(second_summary.batch_rows, 2);
        assert_eq!(second_summary.inserted_rows, 1);

        let cached_state = store.recent_raw_journal_state_cached()?;
        let scanned_state = store.recent_raw_journal_state()?;
        assert_eq!(cached_state, scanned_state);
        assert_eq!(second_summary.row_count, scanned_state.row_count);
        assert_eq!(
            second_summary.covered_through_cursor,
            scanned_state.covered_through_cursor
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_batch_write_with_deadline_returns_bounded_outcome() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("recent-raw-bounded-deadline.db");
        let store = SqliteStore::open(Path::new(&db_path))?;
        let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let swaps = (0..512)
            .map(|idx| {
                swap(
                    &format!("sig-recent-raw-deadline-{idx:04}"),
                    "wallet-deadline",
                    now + Duration::seconds(idx as i64),
                    SOL_MINT,
                    "TokenRecentRawDeadline111111111111111111111",
                    1_000 + idx as u64,
                )
            })
            .collect::<Vec<_>>();

        let (summary, time_budget_exhausted) =
            store.insert_recent_raw_journal_batch_with_deadline(&swaps, now, Instant::now())?;
        assert!(
            time_budget_exhausted,
            "expired deadline must return a bounded outcome instead of hanging in sqlite write path"
        );
        assert_eq!(summary.batch_rows, 0);

        let cached_state = store.recent_raw_journal_state_cached()?;
        let scanned_state = store.recent_raw_journal_state()?;
        assert_eq!(cached_state, scanned_state);
        assert_eq!(summary.row_count, scanned_state.row_count);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_bulk_deadline_write_keeps_cached_state_exact_and_ignores_duplicates(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("recent-raw-bulk-cached-state.db");
        let store = SqliteStore::open(Path::new(&db_path))?;
        let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let existing = swap(
            "sig-recent-raw-bulk-existing",
            "wallet-bulk",
            now,
            SOL_MINT,
            "TokenRecentRawBulkExisting111111111111111111",
            1_000,
        );
        store.insert_recent_raw_journal_batch(std::slice::from_ref(&existing), now)?;

        let mut swaps = vec![existing];
        for idx in 0..130 {
            swaps.push(swap(
                &format!("sig-recent-raw-bulk-{idx:04}"),
                "wallet-bulk",
                now + Duration::seconds(idx as i64 + 1),
                SOL_MINT,
                "TokenRecentRawBulk11111111111111111111111",
                1_001 + idx as u64,
            ));
        }

        let (summary, time_budget_exhausted) = store
            .insert_recent_raw_journal_batch_bulk_with_deadline_internal(
                &swaps,
                now,
                Instant::now() + StdDuration::from_secs(5),
                Some(16),
            )?;
        assert!(!time_budget_exhausted);
        assert_eq!(summary.batch_rows, swaps.len());
        assert_eq!(summary.inserted_rows, swaps.len() - 1);
        assert_eq!(summary.recent_raw_bulk_effective_statement_chunk_rows, 16);
        assert_eq!(summary.recent_raw_bulk_statement_count, 9);
        assert_eq!(summary.recent_raw_bulk_rows_processed, swaps.len());
        assert_eq!(summary.recent_raw_bulk_rows_inserted, swaps.len() - 1);
        assert!(!summary.recent_raw_bulk_deadline_exhausted_before_statement);
        assert!(!summary.recent_raw_bulk_deadline_exhausted_during_execute);

        let cached_state = store.recent_raw_journal_state_cached()?;
        let scanned_state = store.recent_raw_journal_state()?;
        assert_eq!(cached_state, scanned_state);
        assert_eq!(summary.row_count, scanned_state.row_count);
        assert_eq!(
            summary.covered_through_cursor,
            scanned_state.covered_through_cursor
        );
        assert_eq!(scanned_state.row_count, 131);
        assert_eq!(
            scanned_state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("sig-recent-raw-bulk-0129")
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_bulk_default_path_uses_adaptive_effective_chunk_size() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("recent-raw-bulk-adaptive-default.db");
        let store = SqliteStore::open(Path::new(&db_path))?;
        let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let expected_chunk_rows = recent_raw_journal_effective_bulk_insert_chunk_rows(
            None,
            recent_raw_journal_sqlite_variable_limit(&store.conn),
        );
        assert!(
            expected_chunk_rows > 64,
            "test SQLite variable limit should permit exercising a chunk larger than the old 64-row ceiling; got {expected_chunk_rows}"
        );
        let swaps = (0..(expected_chunk_rows + 8))
            .map(|idx| {
                swap(
                    &format!("sig-recent-raw-bulk-adaptive-{idx:04}"),
                    "wallet-bulk-adaptive",
                    now + Duration::seconds(idx as i64),
                    SOL_MINT,
                    "TokenRecentRawBulkAdaptive111111111111",
                    20_000 + idx as u64,
                )
            })
            .collect::<Vec<_>>();
        let _guard = super::install_recent_raw_bulk_write_budget_hook(
            move |processed_rows, inserted_rows| {
                processed_rows >= expected_chunk_rows && inserted_rows >= expected_chunk_rows
            },
        );

        let (summary, time_budget_exhausted) = store
            .insert_recent_raw_journal_batch_bulk_with_deadline(
                &swaps,
                now,
                Instant::now() + StdDuration::from_secs(5),
            )?;

        assert!(time_budget_exhausted);
        assert_eq!(summary.batch_rows, expected_chunk_rows);
        assert_eq!(summary.inserted_rows, expected_chunk_rows);
        assert_eq!(
            summary.recent_raw_bulk_effective_statement_chunk_rows,
            expected_chunk_rows
        );
        assert_eq!(summary.recent_raw_bulk_statement_count, 1);
        assert_eq!(
            summary.recent_raw_bulk_statement_params_per_row,
            RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW
        );
        assert_eq!(
            summary.recent_raw_bulk_statement_chunk_row_cap,
            RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS
        );
        assert_eq!(summary.recent_raw_bulk_rows_processed, expected_chunk_rows);
        assert_eq!(summary.recent_raw_bulk_rows_inserted, expected_chunk_rows);
        assert!(summary.recent_raw_bulk_sqlite_variable_limit >= expected_chunk_rows * 13);
        let cached_state = store.recent_raw_journal_state_cached()?;
        let scanned_state = store.recent_raw_journal_state()?;
        assert_eq!(cached_state, scanned_state);
        assert_eq!(summary.row_count, scanned_state.row_count);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_bulk_deadline_write_preserves_partial_chunks_for_budget_retry(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("recent-raw-bulk-partial-budget.db");
        let store = SqliteStore::open(Path::new(&db_path))?;
        let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let swaps = (0..130)
            .map(|idx| {
                swap(
                    &format!("sig-recent-raw-bulk-partial-{idx:04}"),
                    "wallet-bulk-partial",
                    now + Duration::seconds(idx as i64),
                    SOL_MINT,
                    "TokenRecentRawBulkPartial111111111111111",
                    5_000 + idx as u64,
                )
            })
            .collect::<Vec<_>>();
        let _guard =
            super::install_recent_raw_bulk_write_budget_hook(|processed_rows, inserted_rows| {
                processed_rows >= 16 && inserted_rows >= 16
            });

        let (summary, time_budget_exhausted) = store
            .insert_recent_raw_journal_batch_bulk_with_deadline_internal(
                &swaps,
                now,
                Instant::now() + StdDuration::from_secs(5),
                Some(16),
            )?;
        assert!(time_budget_exhausted);
        assert_eq!(summary.batch_rows, 16);
        assert_eq!(summary.inserted_rows, 16);
        assert_eq!(summary.recent_raw_bulk_effective_statement_chunk_rows, 16);
        assert_eq!(summary.recent_raw_bulk_statement_count, 1);
        assert_eq!(summary.recent_raw_bulk_rows_processed, 16);
        assert_eq!(summary.recent_raw_bulk_rows_inserted, 16);
        assert!(!summary.recent_raw_bulk_deadline_exhausted_before_statement);
        assert!(!summary.recent_raw_bulk_deadline_exhausted_during_execute);

        let cached_state = store.recent_raw_journal_state_cached()?;
        let scanned_state = store.recent_raw_journal_state()?;
        assert_eq!(cached_state, scanned_state);
        assert_eq!(summary.row_count, 16);
        assert_eq!(summary.row_count, scanned_state.row_count);
        assert_eq!(
            scanned_state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("sig-recent-raw-bulk-partial-0015")
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_bulk_deadline_write_returns_bounded_on_expired_deadline() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("recent-raw-bulk-expired-deadline.db");
        let store = SqliteStore::open(Path::new(&db_path))?;
        let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let swaps = (0..512)
            .map(|idx| {
                swap(
                    &format!("sig-recent-raw-bulk-deadline-{idx:04}"),
                    "wallet-bulk-deadline",
                    now + Duration::seconds(idx as i64),
                    SOL_MINT,
                    "TokenRecentRawBulkDeadline111111111111111",
                    10_000 + idx as u64,
                )
            })
            .collect::<Vec<_>>();

        let (summary, time_budget_exhausted) = store
            .insert_recent_raw_journal_batch_bulk_with_deadline_internal(
                &swaps,
                now,
                Instant::now(),
                Some(16),
            )?;
        assert!(
            time_budget_exhausted,
            "expired deadline must return a bounded outcome instead of hanging in optimized recent_raw write path"
        );
        assert_eq!(summary.batch_rows, 0);
        assert_eq!(summary.recent_raw_bulk_statement_count, 0);
        assert!(summary.recent_raw_bulk_deadline_exhausted_before_statement);
        assert!(!summary.recent_raw_bulk_deadline_exhausted_during_execute);

        let cached_state = store.recent_raw_journal_state_cached()?;
        let scanned_state = store.recent_raw_journal_state()?;
        assert_eq!(cached_state, scanned_state);
        assert_eq!(summary.row_count, scanned_state.row_count);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_bulk_deadline_write_preserves_generic_sqlite_errors_as_hard_failures(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("recent-raw-bulk-hard-failure.db");
        let store = SqliteStore::open(Path::new(&db_path))?;
        store.ensure_recent_raw_journal_tables()?;
        store.conn.execute_batch(
            "CREATE TRIGGER recent_raw_bulk_hard_failure
             BEFORE INSERT ON observed_swaps
             BEGIN
                 SELECT RAISE(FAIL, 'synthetic non-timeout recent_raw bulk failure');
             END;",
        )?;
        let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let swaps = vec![swap(
            "sig-recent-raw-bulk-hard-failure",
            "wallet-bulk-hard-failure",
            now,
            SOL_MINT,
            "TokenRecentRawBulkHardFailure1111111111",
            30_000,
        )];

        let error = store
            .insert_recent_raw_journal_batch_bulk_with_deadline_internal(
                &swaps,
                now,
                Instant::now() + StdDuration::from_secs(5),
                Some(16),
            )
            .expect_err("non-timeout sqlite errors must remain hard failures");

        let error = error
            .chain()
            .map(|cause| cause.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            error.contains("failed to bulk insert observed swaps into recent raw journal batch"),
            "unexpected error: {error}"
        );
        assert!(
            error.contains("synthetic non-timeout recent_raw bulk failure"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_cached_read_only_required_uses_cached_state_without_scanning_observed_swaps(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("recent-raw-cached-read-only.db");
        let store = SqliteStore::open(Path::new(&db_path))?;
        let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let swaps = vec![
            swap(
                "sig-recent-raw-cached-read-a",
                "wallet-cached-read",
                now - Duration::minutes(2),
                SOL_MINT,
                "TokenRecentRawCachedReadA11111111111111111111",
                301,
            ),
            swap(
                "sig-recent-raw-cached-read-b",
                "wallet-cached-read",
                now - Duration::minutes(1),
                SOL_MINT,
                "TokenRecentRawCachedReadB11111111111111111111",
                302,
            ),
        ];
        store.insert_recent_raw_journal_batch(&swaps, now)?;
        let expected_state = store.recent_raw_journal_state_cached()?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute("DELETE FROM observed_swaps", [])?;

        let read_only = SqliteStore::open_read_only(Path::new(&db_path))?;
        let cached_state = read_only.recent_raw_journal_state_cached_read_only_required()?;
        assert_eq!(cached_state, expected_state);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_cached_read_only_required_errors_when_cached_state_row_is_missing(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("recent-raw-missing-cached-read-only.db");
        let store = SqliteStore::open(Path::new(&db_path))?;
        let now = DateTime::parse_from_rfc3339("2026-03-29T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let swaps = vec![swap(
            "sig-recent-raw-missing-cached",
            "wallet-missing-cached",
            now - Duration::minutes(1),
            SOL_MINT,
            "TokenRecentRawMissingCached111111111111111111",
            401,
        )];
        store.insert_recent_raw_journal_batch(&swaps, now)?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute("DELETE FROM recent_raw_journal_state WHERE id = 1", [])?;

        let read_only = SqliteStore::open_read_only(Path::new(&db_path))?;
        let error = read_only
            .recent_raw_journal_state_cached_read_only_required()
            .expect_err("missing cached state row must fail explicitly");
        assert!(
            error
                .to_string()
                .contains("cached recent raw journal state row id=1 is missing"),
            "{error:#}"
        );
        Ok(())
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

    #[test]
    fn observed_wallet_activity_page_for_exact_wallets_filters_and_resumes_in_order_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-activity-page-exact-wallet-filter.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let since = DateTime::parse_from_rfc3339("2026-04-11T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let until = since + Duration::hours(4);
        store.insert_observed_swap(&swap(
            "sig-exact-wallet-a-1",
            "wallet-exact-a",
            since + Duration::minutes(10),
            SOL_MINT,
            "TokenExactWalletA11111111111111111111111111",
            1,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-exact-wallet-a-2",
            "wallet-exact-a",
            since + Duration::minutes(20),
            "TokenExactWalletA11111111111111111111111111",
            SOL_MINT,
            2,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-exact-wallet-b-1",
            "wallet-exact-b",
            since + Duration::minutes(30),
            SOL_MINT,
            "TokenExactWalletB11111111111111111111111111",
            3,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-exact-wallet-irrelevant",
            "wallet-exact-irrelevant",
            since + Duration::minutes(40),
            SOL_MINT,
            "TokenExactWalletIrrelevant1111111111111111",
            4,
        ))?;

        let exact_wallet_ids = vec!["wallet-exact-b".to_string(), "wallet-exact-a".to_string()];
        let first_page = store
            .observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
                &exact_wallet_ids,
                since,
                until,
                None,
                1,
                50,
                Instant::now() + StdDuration::from_secs(5),
            )?;
        assert!(!first_page.time_budget_exhausted);
        assert_eq!(first_page.rows_seen, 2);
        assert_eq!(first_page.rows.len(), 1);
        assert_eq!(first_page.rows[0].wallet_id, "wallet-exact-a");
        assert_eq!(first_page.rows[0].trades, 2);

        let second_page = store
            .observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
                &exact_wallet_ids,
                since,
                until,
                Some("wallet-exact-a"),
                1,
                50,
                Instant::now() + StdDuration::from_secs(5),
            )?;
        assert!(!second_page.time_budget_exhausted);
        assert_eq!(second_page.rows_seen, 1);
        assert_eq!(second_page.rows.len(), 1);
        assert_eq!(second_page.rows[0].wallet_id, "wallet-exact-b");
        assert_eq!(second_page.rows[0].trades, 1);
        Ok(())
    }

    #[test]
    fn observed_wallet_activity_target_wallet_filter_reuses_identical_wallet_set_across_pages_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-activity-exact-wallet-filter-cache-reuse.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let since = DateTime::parse_from_rfc3339("2026-04-11T09:30:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let until = since + Duration::hours(2);
        store.insert_observed_swap(&swap(
            "sig-wallet-filter-cache-a",
            "wallet-filter-cache-a",
            since + Duration::minutes(5),
            SOL_MINT,
            "TokenWalletFilterCacheA11111111111111111111",
            1,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-filter-cache-b",
            "wallet-filter-cache-b",
            since + Duration::minutes(10),
            SOL_MINT,
            "TokenWalletFilterCacheB11111111111111111111",
            2,
        ))?;

        let exact_wallet_ids = vec![
            "wallet-filter-cache-b".to_string(),
            "wallet-filter-cache-a".to_string(),
        ];
        let before_first = store.conn.total_changes();
        let first_page = store
            .observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
                &exact_wallet_ids,
                since,
                until,
                None,
                1,
                50,
                Instant::now() + StdDuration::from_secs(5),
            )?;
        let after_first = store.conn.total_changes();
        let second_page = store
            .observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
                &exact_wallet_ids,
                since,
                until,
                Some("wallet-filter-cache-a"),
                1,
                50,
                Instant::now() + StdDuration::from_secs(5),
            )?;
        let after_second = store.conn.total_changes();

        assert_eq!(first_page.rows.len(), 1);
        assert_eq!(second_page.rows.len(), 1);
        assert!(
            after_first > before_first,
            "the first exact-wallet backfill page must populate the temporary wallet filter"
        );
        assert_eq!(
            after_second, after_first,
            "reusing the same exact candidate-wallet set on the same SQLite connection must not rewrite the temporary wallet filter rows on the next page"
        );
        Ok(())
    }

    #[test]
    fn observed_wallet_activity_target_wallet_filter_reloads_when_wallet_set_changes_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-activity-exact-wallet-filter-cache-invalidate.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let since = DateTime::parse_from_rfc3339("2026-04-11T09:45:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let until = since + Duration::hours(2);
        store.insert_observed_swap(&swap(
            "sig-wallet-filter-invalidate-a",
            "wallet-filter-invalidate-a",
            since + Duration::minutes(5),
            SOL_MINT,
            "TokenWalletFilterInvalidateA1111111111111111",
            1,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-filter-invalidate-b",
            "wallet-filter-invalidate-b",
            since + Duration::minutes(10),
            SOL_MINT,
            "TokenWalletFilterInvalidateB1111111111111111",
            2,
        ))?;

        let first_wallet_ids = vec!["wallet-filter-invalidate-a".to_string()];
        let second_wallet_ids = vec![
            "wallet-filter-invalidate-a".to_string(),
            "wallet-filter-invalidate-b".to_string(),
        ];
        let before_first = store.conn.total_changes();
        store.observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
            &first_wallet_ids,
            since,
            until,
            None,
            1,
            50,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        let after_first = store.conn.total_changes();
        store.observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
            &second_wallet_ids,
            since,
            until,
            None,
            2,
            50,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        let after_second = store.conn.total_changes();

        assert!(
            after_first > before_first,
            "the first exact wallet-set load must populate the temporary filter"
        );
        assert!(
            after_second > after_first,
            "changing the exact candidate-wallet set must invalidate and rewrite the temporary wallet filter"
        );
        Ok(())
    }

    #[test]
    fn observed_wallet_activity_page_interrupt_on_wallet_id_query_preserves_time_budget_exhausted_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-activity-page-wallet-id-interrupt.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let since = DateTime::parse_from_rfc3339("2026-04-11T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let until = since + Duration::hours(6);
        seed_wallet_activity_interrupt_fixture(
            &mut store,
            since,
            25_000,
            "wallet-activity-wallet-id-interrupt",
        )?;

        let (stop_interrupts, interrupter) = spawn_interrupt_loop(
            store.conn.get_interrupt_handle(),
            StdDuration::from_millis(0),
        );
        let page = store.observed_wallet_activity_page_in_window_with_budget(
            since,
            until,
            None,
            900,
            50,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        stop_interrupts.store(true, Ordering::Relaxed);
        interrupter.join().expect("interrupt loop thread panicked");

        assert!(
            page.time_budget_exhausted,
            "an interrupted wallet-id page query on the broad wallet-activity path must surface as time-budget exhaustion instead of a successful empty page"
        );
        assert!(
            page.rows.is_empty(),
            "the interrupted broad wallet-id page must not return a misleading successful wallet row set"
        );
        assert_eq!(page.rows_seen, 0);
        assert_eq!(page.active_day_count_source, None);
        Ok(())
    }

    #[test]
    fn observed_wallet_activity_page_for_exact_wallets_interrupt_on_wallet_id_query_preserves_time_budget_exhausted_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-activity-page-exact-wallet-id-interrupt.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let since = DateTime::parse_from_rfc3339("2026-04-11T10:30:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let until = since + Duration::hours(6);
        let exact_wallet_ids = seed_wallet_activity_interrupt_fixture(
            &mut store,
            since,
            25_000,
            "wallet-activity-exact-wallet-id-interrupt",
        )?;

        let preload = store.observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
            &exact_wallet_ids,
            since,
            until,
            None,
            1,
            50,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert!(
            !preload.time_budget_exhausted && preload.rows.len() == 1,
            "the exact-wallet interrupt repro must preload the candidate-wallet temp filter successfully before targeting the wallet-id page seam"
        );

        let page = store.observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
            &exact_wallet_ids,
            since,
            until,
            None,
            900,
            50,
            Instant::now() + StdDuration::from_millis(1),
        )?;

        assert!(
            page.time_budget_exhausted,
            "an interrupted exact-wallet wallet-id page query must remain a time-budget outcome instead of collapsing into an empty successful page"
        );
        assert!(
            page.rows.is_empty(),
            "the interrupted exact-wallet wallet-id page must not return a misleading successful wallet row set"
        );
        assert_eq!(page.rows_seen, 0);
        assert_eq!(page.active_day_count_source, None);
        Ok(())
    }

    #[test]
    fn observed_sol_leg_target_buy_mint_filter_old_like_reload_rewrites_identical_target_set_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("observed-sol-leg-target-filter-old-like-reload.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-11T08:10:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        store.insert_observed_swap(&swap(
            "sig-target-filter-old-like",
            "wallet-target-filter",
            now,
            SOL_MINT,
            "TokenTargetFilterPrimary1111111111111111111",
            1,
        ))?;

        let target_buy_mints: Vec<String> = (0..256)
            .map(|idx| format!("TokenTargetFilterSet{idx:04}11111111111111111111111"))
            .collect();

        let before_first = store.conn.total_changes();
        store.replace_observed_sol_leg_target_buy_mint_filter(&target_buy_mints)?;
        let after_first = store.conn.total_changes();
        store.replace_observed_sol_leg_target_buy_mint_filter(&target_buy_mints)?;
        let after_second = store.conn.total_changes();

        assert!(
            after_first > before_first,
            "old-like setup must write the temporary exact-target filter rows on first load"
        );
        assert!(
            after_second > after_first,
            "old-like behavior must rewrite the same exact target set again instead of reusing the already-loaded temporary filter"
        );
        Ok(())
    }

    #[test]
    fn observed_sol_leg_target_buy_mint_filter_reuses_identical_target_set_across_pages_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("observed-sol-leg-target-filter-cache-reuse.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-11T08:15:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        store.insert_observed_swap(&swap(
            "sig-target-filter-cache-a",
            "wallet-target-filter",
            now,
            SOL_MINT,
            "TokenTargetFilterPrimary1111111111111111111",
            1,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-target-filter-cache-b",
            "wallet-target-filter",
            now + Duration::seconds(1),
            "TokenTargetFilterPrimary1111111111111111111",
            SOL_MINT,
            2,
        ))?;

        let mut target_buy_mints: Vec<String> = (0..512)
            .map(|idx| format!("TokenTargetFilterSet{idx:04}11111111111111111111111"))
            .collect();
        target_buy_mints.push("TokenTargetFilterPrimary1111111111111111111".to_string());

        let before_first = store.conn.total_changes();
        let first_page = store
            .for_each_observed_sol_leg_swap_in_window_after_cursor_for_target_buy_mints_with_budget(
                now - Duration::seconds(1),
                now + Duration::seconds(2),
                None,
                &target_buy_mints,
                1,
                Instant::now() + StdDuration::from_secs(5),
                |_swap| Ok(()),
            )?;
        let after_first = store.conn.total_changes();
        let second_page = store
            .for_each_observed_sol_leg_swap_in_window_after_cursor_for_target_buy_mints_with_budget(
                now - Duration::seconds(1),
                now + Duration::seconds(2),
                Some(&DiscoveryRuntimeCursor {
                    ts_utc: now,
                    slot: 1,
                    signature: "sig-target-filter-cache-a".to_string(),
                }),
                &target_buy_mints,
                1,
                Instant::now() + StdDuration::from_secs(5),
                |_swap| Ok(()),
            )?;
        let after_second = store.conn.total_changes();

        assert_eq!(first_page.rows_seen, 1);
        assert_eq!(second_page.rows_seen, 1);
        assert!(
            after_first > before_first,
            "the first exact-target page must populate the temporary filter cache"
        );
        assert_eq!(
            after_second, after_first,
            "reusing the same exact target set on the same SQLite connection must not rewrite the temporary filter rows on the next replay page"
        );
        Ok(())
    }

    #[test]
    fn observed_sol_leg_target_buy_mint_filter_reloads_when_target_set_changes_stage1() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("observed-sol-leg-target-filter-cache-invalidate.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-11T08:20:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        store.insert_observed_swap(&swap(
            "sig-target-filter-cache-invalidate",
            "wallet-target-filter",
            now,
            SOL_MINT,
            "TokenTargetFilterReload111111111111111111111",
            1,
        ))?;

        let first_target_buy_mints =
            vec!["TokenTargetFilterReload111111111111111111111".to_string()];
        let second_target_buy_mints = vec![
            "TokenTargetFilterReload111111111111111111111".to_string(),
            "TokenTargetFilterReloadSecond1111111111111111".to_string(),
        ];

        let before_first = store.conn.total_changes();
        let _ = store
            .for_each_observed_sol_leg_swap_in_window_after_cursor_for_target_buy_mints_with_budget(
                now - Duration::seconds(1),
                now + Duration::seconds(1),
                None,
                &first_target_buy_mints,
                1,
                Instant::now() + StdDuration::from_secs(5),
                |_swap| Ok(()),
            )?;
        let after_first = store.conn.total_changes();
        let _ = store
            .for_each_observed_sol_leg_swap_in_window_after_cursor_for_target_buy_mints_with_budget(
                now - Duration::seconds(1),
                now + Duration::seconds(1),
                None,
                &second_target_buy_mints,
                1,
                Instant::now() + StdDuration::from_secs(5),
                |_swap| Ok(()),
            )?;
        let after_second = store.conn.total_changes();

        assert!(
            after_first > before_first,
            "the first target set load must write the temporary filter rows"
        );
        assert!(
            after_second > after_first,
            "changing the exact target set must invalidate and reload the temporary filter cache instead of silently reusing stale membership"
        );
        Ok(())
    }
}
