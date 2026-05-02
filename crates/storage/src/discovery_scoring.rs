use super::{
    DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, DiscoveryScoringBatchStageTimings,
    DiscoveryScoringBoundarySeedLot, DiscoveryScoringBoundarySeedSnapshot,
    DiscoveryScoringSeedBoundaryInstallMarker, SqliteBatchedDeleteSummary, SqliteStore,
    TokenMarketStats, WalletScoringBuyFactRow, WalletScoringCloseFactRow, WalletScoringDayRow,
    WalletScoringQualitySource, WalletScoringSnapshot,
};
use crate::market_data::OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use copybot_core_types::SwapEvent;
use rusqlite::{params, Connection, OptionalExtension};
#[cfg(debug_assertions)]
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::{Duration as StdDuration, Instant};

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;
const QUALITY_RPC_TIMEOUT_MS: u64 = 700;
const QUALITY_MAX_SIGNATURE_PAGES: u32 = 1;
const QUALITY_MAX_FETCH_PER_BATCH: usize = 20;
const QUALITY_RPC_BUDGET_MS: u64 = 1_500;
const DISCOVERY_SCORING_PREPARE_PROGRESS_OPS: i32 = 10_000;
const DISCOVERY_SCORING_PREPARE_RUNTIME_BUDGET_EXHAUSTED_REASON: &str =
    "discovery_scoring_prepare_runtime_budget_exhausted";
const DISCOVERY_AGGREGATE_REPAIR_LOCK_FIRST_BUDGET_EXHAUSTED_WITHOUT_PROGRESS: &str =
    "discovery_aggregate_repair_lock_first_budget_exhausted_without_progress";
const DISCOVERY_SCORING_LOCK_FIRST_REPAIR_QUERY_PAGE_ROWS: usize = 512;
const RUG_LOOKAHEAD_STATS_QUERY: &str = "SELECT
                COALESCE(SUM(sol_notional), 0.0) AS volume_sol,
                COUNT(DISTINCT wallet_id) AS unique_traders
             FROM (
                SELECT wallet_id, qty_out AS sol_notional
                FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                WHERE token_in = ?1
                  AND token_out = ?2
                  AND ts >= ?3
                  AND ts <= ?4
                UNION ALL
                SELECT wallet_id, qty_in AS sol_notional
                FROM observed_swaps INDEXED BY idx_observed_swaps_token_out_in_ts
                WHERE token_out = ?1
                  AND token_in = ?2
                  AND ts >= ?3
                  AND ts <= ?4
             )";

#[derive(Debug, Clone)]
struct OpenLotRow {
    buy_signature: String,
    qty: f64,
    cost_sol: f64,
    opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct QualitySnapshot {
    source: WalletScoringQualitySource,
    token_age_seconds: Option<u64>,
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
}

#[derive(Debug, Clone)]
struct QualityCacheRowLocal {
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
    token_age_seconds: Option<u64>,
    fetched_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
struct QualityFetchBudget {
    rpc_attempted: usize,
    started_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct QualityCacheUpsert {
    mint: String,
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
    token_age_seconds: Option<u64>,
    fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct PreparedBuyFact {
    market_stats: TokenMarketStats,
    quality: QualitySnapshot,
    quality_cache_upsert: Option<QualityCacheUpsert>,
    rug_check_after_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct PreparedScoringSwap {
    swap: SwapEvent,
    buy_fact: Option<PreparedBuyFact>,
}

#[derive(Debug, Clone)]
struct CarryoverLotRow {
    qty: f64,
    cost_sol: f64,
    oldest_opened_ts: DateTime<Utc>,
}

#[cfg(debug_assertions)]
thread_local! {
    static DISCOVERY_SCORING_FAIL_AFTER_MATERIALIZATION_BEFORE_CHECKPOINT: Cell<bool> =
        const { Cell::new(false) };
    static DISCOVERY_SCORING_FORCE_PREPARE_RUNTIME_BUDGET_EXHAUSTED: Cell<bool> =
        const { Cell::new(false) };
    static DISCOVERY_SCORING_LOCK_FIRST_REPAIR_BUDGET_AFTER_ROWS: Cell<Option<usize>> =
        const { Cell::new(None) };
    static DISCOVERY_SCORING_RUG_LOOKAHEAD_BUDGET_FAIL_ABOVE_ROWS: Cell<Option<usize>> =
        const { Cell::new(None) };
    static DISCOVERY_SCORING_RUG_LOOKAHEAD_UNKNOWN_FAILPOINT: Cell<bool> =
        const { Cell::new(false) };
    static DISCOVERY_SCORING_LOCK_FIRST_REPAIR_CURRENT_ROWS: Cell<usize> =
        const { Cell::new(0) };
}

#[cfg(debug_assertions)]
fn set_discovery_scoring_atomic_checkpoint_failpoint(enabled: bool) {
    DISCOVERY_SCORING_FAIL_AFTER_MATERIALIZATION_BEFORE_CHECKPOINT
        .with(|failpoint| failpoint.set(enabled));
}

#[cfg(debug_assertions)]
pub(crate) fn maybe_fail_after_materialization_before_checkpoint() -> Result<()> {
    let fired = DISCOVERY_SCORING_FAIL_AFTER_MATERIALIZATION_BEFORE_CHECKPOINT.with(|failpoint| {
        let fired = failpoint.get();
        failpoint.set(false);
        fired
    });
    if fired {
        anyhow::bail!(
            "test failpoint: discovery scoring crash after materialization before checkpoint"
        );
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
pub(crate) fn maybe_fail_after_materialization_before_checkpoint() -> Result<()> {
    Ok(())
}

#[cfg(debug_assertions)]
impl SqliteStore {
    #[doc(hidden)]
    pub fn set_discovery_scoring_atomic_checkpoint_failpoint_for_tests(enabled: bool) {
        set_discovery_scoring_atomic_checkpoint_failpoint(enabled);
    }

    #[doc(hidden)]
    pub fn set_discovery_scoring_prepare_runtime_budget_failpoint_for_tests(enabled: bool) {
        DISCOVERY_SCORING_FORCE_PREPARE_RUNTIME_BUDGET_EXHAUSTED
            .with(|failpoint| failpoint.set(enabled));
    }

    #[doc(hidden)]
    pub fn set_discovery_scoring_lock_first_repair_budget_after_rows_for_tests(
        rows: Option<usize>,
    ) {
        DISCOVERY_SCORING_LOCK_FIRST_REPAIR_BUDGET_AFTER_ROWS.with(|failpoint| failpoint.set(rows));
    }

    #[doc(hidden)]
    pub fn set_discovery_scoring_rug_lookahead_budget_fail_above_rows_for_tests(
        rows: Option<usize>,
    ) {
        DISCOVERY_SCORING_RUG_LOOKAHEAD_BUDGET_FAIL_ABOVE_ROWS
            .with(|failpoint| failpoint.set(rows));
    }

    #[doc(hidden)]
    pub fn set_discovery_scoring_rug_lookahead_unknown_failpoint_for_tests(enabled: bool) {
        DISCOVERY_SCORING_RUG_LOOKAHEAD_UNKNOWN_FAILPOINT.with(|failpoint| failpoint.set(enabled));
    }
}

struct DiscoveryScoringPrepareProgressGuard<'a> {
    conn: &'a Connection,
}

impl<'a> DiscoveryScoringPrepareProgressGuard<'a> {
    fn install(conn: &'a Connection, deadline: Instant) -> Self {
        conn.progress_handler(
            DISCOVERY_SCORING_PREPARE_PROGRESS_OPS,
            Some(move || Instant::now() >= deadline),
        );
        Self { conn }
    }
}

impl Drop for DiscoveryScoringPrepareProgressGuard<'_> {
    fn drop(&mut self) {
        self.conn.progress_handler(0, None::<fn() -> bool>);
    }
}

fn is_sol_buy(swap: &SwapEvent) -> bool {
    swap.token_in == SOL_MINT && swap.token_out != SOL_MINT
}

fn is_sol_sell(swap: &SwapEvent) -> bool {
    swap.token_out == SOL_MINT && swap.token_in != SOL_MINT
}

fn cmp_swap_order(a: &SwapEvent, b: &SwapEvent) -> Ordering {
    a.ts_utc
        .cmp(&b.ts_utc)
        .then_with(|| a.slot.cmp(&b.slot))
        .then_with(|| a.signature.cmp(&b.signature))
}

fn cmp_cursor_order(a: &DiscoveryRuntimeCursor, b: &DiscoveryRuntimeCursor) -> Ordering {
    a.ts_utc
        .cmp(&b.ts_utc)
        .then_with(|| a.slot.cmp(&b.slot))
        .then_with(|| a.signature.cmp(&b.signature))
}

fn upsert_discovery_scoring_state_value_on_conn(
    conn: &Connection,
    state_key: &str,
    state_value: &str,
    updated_at: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(state_key) DO UPDATE SET
            state_value = excluded.state_value,
            updated_at = excluded.updated_at",
        params![state_key, state_value, updated_at],
    )
    .with_context(|| format!("failed upserting discovery_scoring_state.{state_key}"))?;
    Ok(())
}

fn upsert_discovery_scoring_cursor_state_on_conn(
    conn: &Connection,
    ts_key: &str,
    slot_key: &str,
    signature_key: &str,
    cursor: &DiscoveryRuntimeCursor,
    updated_at: &str,
) -> Result<()> {
    upsert_discovery_scoring_state_value_on_conn(
        conn,
        ts_key,
        &cursor.ts_utc.to_rfc3339(),
        updated_at,
    )?;
    upsert_discovery_scoring_state_value_on_conn(
        conn,
        slot_key,
        &cursor.slot.to_string(),
        updated_at,
    )?;
    upsert_discovery_scoring_state_value_on_conn(
        conn,
        signature_key,
        &cursor.signature,
        updated_at,
    )?;
    Ok(())
}

pub(crate) fn upsert_discovery_scoring_backfill_progress_on_conn(
    conn: &Connection,
    start_ts: DateTime<Utc>,
    cursor: &DiscoveryRuntimeCursor,
    updated_at: &str,
) -> Result<()> {
    upsert_discovery_scoring_state_value_on_conn(
        conn,
        "backfill_progress_start_ts",
        &start_ts.to_rfc3339(),
        updated_at,
    )?;
    upsert_discovery_scoring_cursor_state_on_conn(
        conn,
        "backfill_progress_cursor_ts",
        "backfill_progress_cursor_slot",
        "backfill_progress_cursor_signature",
        cursor,
        updated_at,
    )?;
    Ok(())
}

fn upsert_discovery_scoring_seed_boundary_install_marker_on_conn(
    conn: &Connection,
    start_ts: DateTime<Utc>,
    cursor: &DiscoveryRuntimeCursor,
    updated_at: &str,
) -> Result<()> {
    upsert_discovery_scoring_state_value_on_conn(
        conn,
        "seed_boundary_install_start_ts",
        &start_ts.to_rfc3339(),
        updated_at,
    )?;
    upsert_discovery_scoring_cursor_state_on_conn(
        conn,
        "seed_boundary_install_cursor_ts",
        "seed_boundary_install_cursor_slot",
        "seed_boundary_install_cursor_signature",
        cursor,
        updated_at,
    )?;
    Ok(())
}

fn clear_discovery_scoring_seed_boundary_install_marker_on_conn(conn: &Connection) -> Result<()> {
    conn.execute(
        "DELETE FROM discovery_scoring_state
         WHERE state_key IN (
            'seed_boundary_install_start_ts',
            'seed_boundary_install_cursor_ts',
            'seed_boundary_install_cursor_slot',
            'seed_boundary_install_cursor_signature'
         )",
        [],
    )
    .context("failed clearing discovery_scoring_state.seed_boundary_install marker")?;
    Ok(())
}

fn upsert_discovery_scoring_materialization_gap_cursor_on_conn(
    conn: &Connection,
    cursor: &DiscoveryRuntimeCursor,
    updated_at: &str,
) -> Result<()> {
    upsert_discovery_scoring_cursor_state_on_conn(
        conn,
        "materialization_gap_since_ts",
        "materialization_gap_since_slot",
        "materialization_gap_since_signature",
        cursor,
        updated_at,
    )?;
    Ok(())
}

fn clear_discovery_scoring_materialization_gap_repair_target_on_conn(
    conn: &Connection,
) -> Result<()> {
    conn.execute(
        "DELETE FROM discovery_scoring_state
         WHERE state_key IN (
            'materialization_gap_repair_gap_ts',
            'materialization_gap_repair_gap_slot',
            'materialization_gap_repair_gap_signature',
            'materialization_gap_repair_target_ts',
            'materialization_gap_repair_target_slot',
            'materialization_gap_repair_target_signature'
         )",
        [],
    )
    .context("failed clearing discovery_scoring_state.materialization_gap repair target")?;
    Ok(())
}

fn load_discovery_scoring_state_value_on_conn(
    conn: &Connection,
    state_key: &str,
) -> Result<Option<String>> {
    conn.query_row(
        "SELECT state_value
         FROM discovery_scoring_state
         WHERE state_key = ?1",
        params![state_key],
        |row| row.get(0),
    )
    .optional()
    .with_context(|| format!("failed querying discovery_scoring_state.{state_key}"))
}

fn load_discovery_scoring_cursor_state_exact_on_conn(
    conn: &Connection,
    ts_key: &str,
    slot_key: &str,
    signature_key: &str,
    label: &str,
) -> Result<Option<DiscoveryRuntimeCursor>> {
    let ts_raw = load_discovery_scoring_state_value_on_conn(conn, ts_key)?;
    let slot_raw = load_discovery_scoring_state_value_on_conn(conn, slot_key)?;
    let signature = load_discovery_scoring_state_value_on_conn(conn, signature_key)?;
    match (ts_raw, slot_raw, signature) {
        (None, None, None) => Ok(None),
        (Some(ts_raw), Some(slot_raw), Some(signature)) => {
            let ts_utc = parse_ts(&ts_raw, &format!("discovery_scoring_state.{ts_key}"))?;
            let slot = slot_raw.parse::<u64>().with_context(|| {
                format!("invalid discovery_scoring_state.{slot_key} value: {slot_raw}")
            })?;
            Ok(Some(DiscoveryRuntimeCursor {
                ts_utc,
                slot,
                signature,
            }))
        }
        _ => Err(anyhow!(
            "discovery_scoring_state.{label} cursor is partially populated"
        )),
    }
}

fn load_discovery_scoring_materialization_gap_cursor_on_conn(
    conn: &Connection,
) -> Result<Option<DiscoveryRuntimeCursor>> {
    load_discovery_scoring_cursor_state_exact_on_conn(
        conn,
        "materialization_gap_since_ts",
        "materialization_gap_since_slot",
        "materialization_gap_since_signature",
        "materialization_gap_since",
    )
}

fn load_discovery_scoring_materialization_gap_repair_target_on_conn(
    conn: &Connection,
) -> Result<Option<(DiscoveryRuntimeCursor, DiscoveryRuntimeCursor)>> {
    let gap_cursor = load_discovery_scoring_cursor_state_exact_on_conn(
        conn,
        "materialization_gap_repair_gap_ts",
        "materialization_gap_repair_gap_slot",
        "materialization_gap_repair_gap_signature",
        "materialization_gap_repair_gap",
    )?;
    let target_cursor = load_discovery_scoring_cursor_state_exact_on_conn(
        conn,
        "materialization_gap_repair_target_ts",
        "materialization_gap_repair_target_slot",
        "materialization_gap_repair_target_signature",
        "materialization_gap_repair_target",
    )?;
    match (gap_cursor, target_cursor) {
        (None, None) => Ok(None),
        (Some(gap_cursor), Some(target_cursor)) => Ok(Some((gap_cursor, target_cursor))),
        _ => Err(anyhow!(
            "discovery_scoring_state.materialization_gap_repair target is partially populated"
        )),
    }
}

fn observed_swap_exact_cursor_exists_on_conn(
    conn: &Connection,
    cursor: &DiscoveryRuntimeCursor,
) -> Result<bool> {
    let slot = i64::try_from(cursor.slot).with_context(|| {
        format!(
            "observed_swaps exact cursor slot overflows i64: {}",
            cursor.slot
        )
    })?;
    let found = conn
        .query_row(
            "SELECT 1
             FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
             WHERE ts = ?1 AND slot = ?2 AND signature = ?3
             LIMIT 1",
            params![cursor.ts_utc.to_rfc3339(), slot, cursor.signature.as_str()],
            |_row| Ok(()),
        )
        .optional()
        .context("failed loading observed_swaps exact cursor row")?
        .is_some();
    Ok(found)
}

fn load_observed_swaps_after_cursor_for_repair_on_conn(
    conn: &Connection,
    cursor: &DiscoveryRuntimeCursor,
    repair_target: &DiscoveryRuntimeCursor,
    limit: usize,
) -> Result<(Vec<SwapEvent>, bool)> {
    if limit == 0 {
        return Ok((Vec::new(), false));
    }
    let limit = (limit.min(i64::MAX as usize)) as i64;
    let mut stmt = conn
        .prepare(OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY)
        .context("failed to prepare observed_swaps repair micro page query")?;
    let mut rows = stmt
        .query(params![
            cursor.ts_utc.to_rfc3339(),
            cursor.slot as i64,
            cursor.signature.as_str(),
            limit,
        ])
        .context("failed querying observed_swaps repair micro page")?;
    let mut swaps = Vec::new();
    let mut reached_target = false;
    while let Some(row) = rows
        .next()
        .context("failed iterating observed_swaps repair micro page rows")?
    {
        let swap = SqliteStore::row_to_swap_event(row)?;
        let swap_cursor = DiscoveryRuntimeCursor {
            ts_utc: swap.ts_utc,
            slot: swap.slot,
            signature: swap.signature.clone(),
        };
        match cmp_cursor_order(&swap_cursor, repair_target) {
            Ordering::Greater => break,
            Ordering::Equal => {
                reached_target = true;
                swaps.push(swap);
                break;
            }
            Ordering::Less => swaps.push(swap),
        }
    }
    Ok((swaps, reached_target))
}

#[cfg(debug_assertions)]
fn lock_first_repair_budget_after_rows_for_tests() -> Option<usize> {
    DISCOVERY_SCORING_LOCK_FIRST_REPAIR_BUDGET_AFTER_ROWS.with(|failpoint| failpoint.get())
}

#[cfg(not(debug_assertions))]
fn lock_first_repair_budget_after_rows_for_tests() -> Option<usize> {
    None
}

fn lock_first_repair_budget_reached(collected_rows: usize) -> bool {
    lock_first_repair_budget_after_rows_for_tests().is_some_and(|limit| collected_rows >= limit)
}

#[cfg(debug_assertions)]
struct LockFirstRepairCurrentRowsGuard;

#[cfg(debug_assertions)]
impl LockFirstRepairCurrentRowsGuard {
    fn install(rows: usize) -> Self {
        DISCOVERY_SCORING_LOCK_FIRST_REPAIR_CURRENT_ROWS.with(|current| current.set(rows));
        Self
    }
}

#[cfg(debug_assertions)]
impl Drop for LockFirstRepairCurrentRowsGuard {
    fn drop(&mut self) {
        DISCOVERY_SCORING_LOCK_FIRST_REPAIR_CURRENT_ROWS.with(|current| current.set(0));
    }
}

#[cfg(not(debug_assertions))]
struct LockFirstRepairCurrentRowsGuard;

#[cfg(not(debug_assertions))]
impl LockFirstRepairCurrentRowsGuard {
    fn install(_rows: usize) -> Self {
        Self
    }
}

#[cfg(debug_assertions)]
fn rug_lookahead_budget_failpoint_triggered() -> bool {
    let current_rows =
        DISCOVERY_SCORING_LOCK_FIRST_REPAIR_CURRENT_ROWS.with(|current| current.get());
    DISCOVERY_SCORING_RUG_LOOKAHEAD_BUDGET_FAIL_ABOVE_ROWS
        .with(|limit| limit.get())
        .is_some_and(|limit| current_rows > limit)
}

#[cfg(not(debug_assertions))]
fn rug_lookahead_budget_failpoint_triggered() -> bool {
    false
}

#[cfg(debug_assertions)]
fn rug_lookahead_unknown_failpoint_triggered() -> bool {
    DISCOVERY_SCORING_RUG_LOOKAHEAD_UNKNOWN_FAILPOINT.with(|failpoint| failpoint.get())
}

#[cfg(not(debug_assertions))]
fn rug_lookahead_unknown_failpoint_triggered() -> bool {
    false
}

fn check_lock_first_repair_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        anyhow::bail!(DISCOVERY_AGGREGATE_REPAIR_LOCK_FIRST_BUDGET_EXHAUSTED_WITHOUT_PROGRESS);
    }
    Ok(())
}

fn upsert_discovery_scoring_materialization_gap_repair_target_on_conn(
    conn: &Connection,
    gap_cursor: &DiscoveryRuntimeCursor,
    target_cursor: &DiscoveryRuntimeCursor,
    updated_at: &str,
) -> Result<()> {
    upsert_discovery_scoring_cursor_state_on_conn(
        conn,
        "materialization_gap_repair_gap_ts",
        "materialization_gap_repair_gap_slot",
        "materialization_gap_repair_gap_signature",
        gap_cursor,
        updated_at,
    )?;
    upsert_discovery_scoring_cursor_state_on_conn(
        conn,
        "materialization_gap_repair_target_ts",
        "materialization_gap_repair_target_slot",
        "materialization_gap_repair_target_signature",
        target_cursor,
        updated_at,
    )?;
    Ok(())
}

fn wallet_scoring_carryover_lot_count_on_conn(conn: &Connection) -> Result<usize> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
            [],
            |row| row.get(0),
        )
        .context("failed counting wallet_scoring_carryover_lots")?;
    Ok(count.max(0) as usize)
}

fn wallet_scoring_open_lot_count_on_conn(conn: &Connection) -> Result<usize> {
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM wallet_scoring_open_lots", [], |row| {
            row.get(0)
        })
        .context("failed counting wallet_scoring_open_lots")?;
    Ok(count.max(0) as usize)
}

fn load_wallet_scoring_boundary_seed_lots_on_conn(
    conn: &Connection,
) -> Result<Vec<DiscoveryScoringBoundarySeedLot>> {
    let mut stmt = conn
        .prepare(
            "SELECT buy_signature, wallet_id, token, qty, cost_sol, opened_ts
             FROM wallet_scoring_open_lots
             ORDER BY wallet_id ASC, token ASC, opened_ts ASC, buy_signature ASC",
        )
        .context("failed preparing wallet_scoring boundary seed lot query")?;
    let mut rows = stmt
        .query([])
        .context("failed querying wallet_scoring boundary seed lots")?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating wallet_scoring boundary seed lots")?
    {
        let opened_ts_raw: String = row
            .get(5)
            .context("failed reading wallet_scoring boundary seed lot opened_ts")?;
        out.push(DiscoveryScoringBoundarySeedLot {
            buy_signature: row
                .get(0)
                .context("failed reading wallet_scoring boundary seed lot buy_signature")?,
            wallet_id: row
                .get(1)
                .context("failed reading wallet_scoring boundary seed lot wallet_id")?,
            token: row
                .get(2)
                .context("failed reading wallet_scoring boundary seed lot token")?,
            qty: row
                .get(3)
                .context("failed reading wallet_scoring boundary seed lot qty")?,
            cost_sol: row
                .get(4)
                .context("failed reading wallet_scoring boundary seed lot cost_sol")?,
            opened_ts: parse_ts(&opened_ts_raw, "wallet_scoring boundary seed lot opened_ts")?,
        });
    }
    Ok(out)
}

fn parse_day(raw: &str, field: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .with_context(|| format!("invalid {field} date value: {raw}"))
}

fn parse_ts(raw: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid {field} rfc3339 value: {raw}"))
}

fn sanitize_prepare_log_value(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_whitespace() || ch.is_control() {
                '_'
            } else {
                ch
            }
        })
        .collect()
}

fn emit_prepare_stage_start(
    emit: &mut impl FnMut(String),
    stage: &str,
    token: Option<&str>,
    swap_signature: Option<&str>,
) {
    emit(format!(
        "event=backfill_prepare_stage_start stage={} token={} swap_signature={} elapsed_ms=0 outcome=started",
        stage,
        token.map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
        swap_signature
            .map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
    ));
}

fn emit_prepare_stage_end(
    emit: &mut impl FnMut(String),
    stage: &str,
    token: Option<&str>,
    swap_signature: Option<&str>,
    elapsed_ms: u64,
    outcome: &str,
) {
    emit(format!(
        "event=backfill_prepare_stage_end stage={} token={} swap_signature={} elapsed_ms={} outcome={}",
        stage,
        token.map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
        swap_signature
            .map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
        elapsed_ms,
        outcome,
    ));
}

fn emit_prepare_stage_skipped(
    emit: &mut impl FnMut(String),
    stage: &str,
    token: Option<&str>,
    swap_signature: Option<&str>,
    reason: &str,
) {
    emit(format!(
        "event=backfill_prepare_stage_skipped stage={} token={} swap_signature={} elapsed_ms=0 outcome=skipped reason={}",
        stage,
        token.map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
        swap_signature
            .map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
        reason,
    ));
}

fn emit_prepare_batch_counts(emit: &mut impl FnMut(String), swaps: &[SwapEvent]) {
    let buy_count = swaps.iter().filter(|swap| is_sol_buy(swap)).count();
    let sell_count = swaps.iter().filter(|swap| is_sol_sell(swap)).count();
    let other_count = swaps
        .len()
        .saturating_sub(buy_count.saturating_add(sell_count));
    emit(format!(
        "event=backfill_prepare_batch_counts stage=prepare_discovery_scoring_swaps token=none swap_signature=none elapsed_ms=0 outcome=observed total_swaps={} buy_count={} sell_count={} other_count={}",
        swaps.len(),
        buy_count,
        sell_count,
        other_count,
    ));
}

#[cfg(debug_assertions)]
fn forced_prepare_runtime_budget_exhausted() -> bool {
    DISCOVERY_SCORING_FORCE_PREPARE_RUNTIME_BUDGET_EXHAUSTED.with(|failpoint| {
        let fired = failpoint.get();
        failpoint.set(false);
        fired
    })
}

#[cfg(not(debug_assertions))]
fn forced_prepare_runtime_budget_exhausted() -> bool {
    false
}

fn prepare_runtime_budget_exhausted(deadline: Option<Instant>) -> bool {
    forced_prepare_runtime_budget_exhausted()
        || deadline.is_some_and(|deadline| Instant::now() >= deadline)
}

fn prepare_runtime_budget_error(
    stage: &str,
    token: Option<&str>,
    swap_signature: Option<&str>,
) -> anyhow::Error {
    anyhow!(
        "{}:stage={}:token={}:swap_signature={}",
        DISCOVERY_SCORING_PREPARE_RUNTIME_BUDGET_EXHAUSTED_REASON,
        stage,
        token
            .map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
        swap_signature
            .map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string())
    )
}

fn check_prepare_runtime_budget(
    deadline: Option<Instant>,
    emit: &mut impl FnMut(String),
    stage: &str,
    token: Option<&str>,
    swap_signature: Option<&str>,
) -> Result<()> {
    if prepare_runtime_budget_exhausted(deadline) {
        emit_prepare_stage_end(
            emit,
            stage,
            token,
            swap_signature,
            0,
            "runtime_budget_exhausted",
        );
        return Err(prepare_runtime_budget_error(stage, token, swap_signature));
    }
    Ok(())
}

fn prepare_error_is_runtime_budget(error: &anyhow::Error) -> bool {
    format!("{error:#}").contains(DISCOVERY_SCORING_PREPARE_RUNTIME_BUDGET_EXHAUSTED_REASON)
}

fn prepare_stage_error(
    error: anyhow::Error,
    deadline: Option<Instant>,
    stage: &str,
    token: Option<&str>,
    swap_signature: Option<&str>,
) -> anyhow::Error {
    if prepare_runtime_budget_exhausted(deadline) {
        prepare_runtime_budget_error(stage, token, swap_signature).context(error)
    } else {
        error
    }
}

fn token_market_stats_on_conn(
    conn: &Connection,
    token: &str,
    as_of: DateTime<Utc>,
) -> Result<TokenMarketStats> {
    let window_start = (as_of - Duration::minutes(5)).to_rfc3339();
    let window_end = as_of.to_rfc3339();
    let (volume_5m_sol, liquidity_sol_proxy, unique_traders_5m_raw): (f64, f64, i64) = conn
        .query_row(
            "SELECT
                COALESCE(SUM(sol_notional), 0.0) AS volume_5m_sol,
                COALESCE(MAX(sol_notional), 0.0) AS liquidity_sol_proxy,
                COUNT(DISTINCT wallet_id) AS unique_traders_5m
             FROM (
                SELECT wallet_id, qty_out AS sol_notional
                FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                WHERE token_in = ?1
                  AND token_out = ?2
                  AND ts >= ?3
                  AND ts <= ?4
                UNION ALL
                SELECT wallet_id, qty_in AS sol_notional
                FROM observed_swaps INDEXED BY idx_observed_swaps_token_out_in_ts
                WHERE token_out = ?1
                  AND token_in = ?2
                  AND ts >= ?3
                  AND ts <= ?4
             )",
            params![token, SOL_MINT, window_start, window_end],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .context("failed querying token 5m market stats for discovery scoring")?;

    Ok(TokenMarketStats {
        first_seen: None,
        holders_proxy: 0,
        liquidity_sol_proxy,
        volume_5m_sol,
        unique_traders_5m: unique_traders_5m_raw.max(0) as u64,
    })
}

fn load_token_quality_cache_on_conn(
    conn: &Connection,
    mint: &str,
) -> Result<Option<QualityCacheRowLocal>> {
    let row: Option<(Option<i64>, Option<f64>, Option<i64>, String)> = conn
        .query_row(
            "SELECT holders, liquidity_sol, token_age_seconds, fetched_at
             FROM token_quality_cache
             WHERE mint = ?1",
            params![mint],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .context("failed querying token_quality_cache in discovery scoring write path")?;

    row.map(
        |(holders_raw, liquidity_sol, token_age_seconds_raw, fetched_at_raw)| -> Result<_> {
            Ok(QualityCacheRowLocal {
                holders: holders_raw.map(|value| value.max(0) as u64),
                liquidity_sol,
                token_age_seconds: token_age_seconds_raw.map(|value| value.max(0) as u64),
                fetched_at: parse_ts(&fetched_at_raw, "token_quality_cache.fetched_at")?,
            })
        },
    )
    .transpose()
}

fn upsert_token_quality_cache_on_conn(
    conn: &Connection,
    mint: &str,
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
    token_age_seconds: Option<u64>,
    fetched_at: DateTime<Utc>,
) -> Result<()> {
    conn.execute(
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
    .context("failed upserting token_quality_cache row in discovery scoring write path")?;
    Ok(())
}

fn cached_quality_snapshot(
    cached: Option<QualityCacheRowLocal>,
    signal_ts: DateTime<Utc>,
    missing_source: WalletScoringQualitySource,
) -> QualitySnapshot {
    match cached {
        Some(row) => QualitySnapshot {
            source: WalletScoringQualitySource::Stale,
            token_age_seconds: row.token_age_seconds.map(|age| {
                age.saturating_add(
                    signal_ts
                        .signed_duration_since(row.fetched_at)
                        .num_seconds()
                        .max(0) as u64,
                )
            }),
            holders: row.holders,
            liquidity_sol: row.liquidity_sol,
        },
        None => QualitySnapshot {
            source: missing_source,
            token_age_seconds: None,
            holders: None,
            liquidity_sol: None,
        },
    }
}

fn resolve_quality_snapshot_on_conn_with_diagnostics(
    conn: &Connection,
    mint: &str,
    signal_ts: DateTime<Utc>,
    config: &DiscoveryAggregateWriteConfig,
    budget: &mut QualityFetchBudget,
    emit: &mut impl FnMut(String),
    deadline: Option<Instant>,
    swap_signature: Option<&str>,
) -> Result<(QualitySnapshot, Option<QualityCacheUpsert>)> {
    emit_prepare_stage_start(emit, "quality_cache_lookup", Some(mint), swap_signature);
    let cache_started_at = Instant::now();
    check_prepare_runtime_budget(
        deadline,
        emit,
        "quality_cache_lookup",
        Some(mint),
        swap_signature,
    )?;
    let cached = match load_token_quality_cache_on_conn(conn, mint) {
        Ok(cached) => {
            emit_prepare_stage_end(
                emit,
                "quality_cache_lookup",
                Some(mint),
                swap_signature,
                cache_started_at.elapsed().as_millis() as u64,
                "completed",
            );
            cached
        }
        Err(error) => {
            let error = prepare_stage_error(
                error,
                deadline,
                "quality_cache_lookup",
                Some(mint),
                swap_signature,
            );
            emit_prepare_stage_end(
                emit,
                "quality_cache_lookup",
                Some(mint),
                swap_signature,
                cache_started_at.elapsed().as_millis() as u64,
                if prepare_error_is_runtime_budget(&error) {
                    "runtime_budget_exhausted"
                } else {
                    "failed"
                },
            );
            return Err(error);
        }
    };
    let ttl = Duration::seconds(QUALITY_CACHE_TTL_SECONDS);
    if let Some(row) = cached.as_ref() {
        if signal_ts.signed_duration_since(row.fetched_at) <= ttl {
            emit_prepare_stage_skipped(
                emit,
                "quality_rpc_fetch",
                Some(mint),
                swap_signature,
                "fresh_cache_hit",
            );
            return Ok((
                QualitySnapshot {
                    source: WalletScoringQualitySource::Fresh,
                    token_age_seconds: row.token_age_seconds,
                    holders: row.holders,
                    liquidity_sol: row.liquidity_sol,
                },
                None,
            ));
        }
    }

    let Some(helius_http_url) = config.helius_http_url.as_deref() else {
        emit_prepare_stage_skipped(
            emit,
            "quality_rpc_fetch",
            Some(mint),
            swap_signature,
            "helius_http_url_absent",
        );
        return Ok((
            cached_quality_snapshot(cached, signal_ts, WalletScoringQualitySource::Missing),
            None,
        ));
    };

    if budget.rpc_attempted >= QUALITY_MAX_FETCH_PER_BATCH {
        emit_prepare_stage_skipped(
            emit,
            "quality_rpc_fetch",
            Some(mint),
            swap_signature,
            "quality_rpc_max_fetch_per_batch_exhausted",
        );
        return Ok((
            cached_quality_snapshot(cached, signal_ts, WalletScoringQualitySource::Deferred),
            None,
        ));
    }

    let started_at = budget.started_at.get_or_insert_with(Instant::now);
    if started_at.elapsed().as_millis() as u64 >= QUALITY_RPC_BUDGET_MS {
        emit_prepare_stage_skipped(
            emit,
            "quality_rpc_fetch",
            Some(mint),
            swap_signature,
            "quality_rpc_budget_exhausted",
        );
        return Ok((
            cached_quality_snapshot(cached, signal_ts, WalletScoringQualitySource::Deferred),
            None,
        ));
    }

    budget.rpc_attempted = budget.rpc_attempted.saturating_add(1);
    emit_prepare_stage_start(emit, "quality_rpc_fetch", Some(mint), swap_signature);
    let rpc_started_at = Instant::now();
    check_prepare_runtime_budget(
        deadline,
        emit,
        "quality_rpc_fetch",
        Some(mint),
        swap_signature,
    )?;
    let fetched = SqliteStore::fetch_token_quality_from_helius(
        helius_http_url,
        mint,
        QUALITY_RPC_TIMEOUT_MS,
        QUALITY_MAX_SIGNATURE_PAGES,
        config.min_token_age_hint_seconds,
    );
    match fetched {
        Ok(fetched) => {
            emit_prepare_stage_end(
                emit,
                "quality_rpc_fetch",
                Some(mint),
                swap_signature,
                rpc_started_at.elapsed().as_millis() as u64,
                "completed",
            );
            Ok((
                QualitySnapshot {
                    source: WalletScoringQualitySource::Fresh,
                    token_age_seconds: fetched.token_age_seconds,
                    holders: fetched.holders,
                    liquidity_sol: fetched.liquidity_sol,
                },
                Some(QualityCacheUpsert {
                    mint: mint.to_string(),
                    holders: fetched.holders,
                    liquidity_sol: fetched.liquidity_sol,
                    token_age_seconds: fetched.token_age_seconds,
                    fetched_at: signal_ts,
                }),
            ))
        }
        Err(_) => {
            emit_prepare_stage_end(
                emit,
                "quality_rpc_fetch",
                Some(mint),
                swap_signature,
                rpc_started_at.elapsed().as_millis() as u64,
                "failed",
            );
            Ok((
                cached_quality_snapshot(cached, signal_ts, WalletScoringQualitySource::Missing),
                None,
            ))
        }
    }
}

fn upsert_wallet_scoring_day_on_conn(conn: &Connection, swap: &SwapEvent) -> Result<()> {
    let buy_notional = if is_sol_buy(swap) {
        swap.amount_in.max(0.0)
    } else {
        0.0
    };
    conn.execute(
        "INSERT INTO wallet_scoring_days(
            wallet_id,
            activity_day,
            first_seen,
            last_seen,
            trades,
            spent_sol,
            max_buy_notional_sol
         ) VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6)
         ON CONFLICT(wallet_id, activity_day) DO UPDATE SET
            first_seen = CASE
                WHEN excluded.first_seen < wallet_scoring_days.first_seen
                    THEN excluded.first_seen
                ELSE wallet_scoring_days.first_seen
            END,
            last_seen = CASE
                WHEN excluded.last_seen > wallet_scoring_days.last_seen
                    THEN excluded.last_seen
                ELSE wallet_scoring_days.last_seen
            END,
            trades = wallet_scoring_days.trades + 1,
            spent_sol = wallet_scoring_days.spent_sol + excluded.spent_sol,
            max_buy_notional_sol = MAX(
                wallet_scoring_days.max_buy_notional_sol,
                excluded.max_buy_notional_sol
            )",
        params![
            &swap.wallet,
            swap.ts_utc.date_naive().format("%Y-%m-%d").to_string(),
            swap.ts_utc.to_rfc3339(),
            swap.ts_utc.to_rfc3339(),
            buy_notional,
            buy_notional,
        ],
    )
    .context("failed upserting wallet_scoring_days row")?;
    Ok(())
}

fn upsert_wallet_scoring_tx_minute_on_conn(
    conn: &Connection,
    wallet_id: &str,
    minute_bucket: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO wallet_scoring_tx_minutes(wallet_id, minute_bucket, tx_count)
         VALUES (?1, ?2, 1)
         ON CONFLICT(wallet_id, minute_bucket) DO UPDATE SET
            tx_count = wallet_scoring_tx_minutes.tx_count + 1",
        params![wallet_id, minute_bucket],
    )
    .context("failed upserting wallet_scoring_tx_minutes row")?;
    Ok(())
}

fn prepare_discovery_scoring_swaps(
    conn: &Connection,
    swaps: &[SwapEvent],
    config: &DiscoveryAggregateWriteConfig,
) -> Result<Vec<PreparedScoringSwap>> {
    prepare_discovery_scoring_swaps_with_diagnostics(conn, swaps, config, &mut |_| {}, None)
}

fn prepare_discovery_scoring_swaps_with_diagnostics(
    conn: &Connection,
    swaps: &[SwapEvent],
    config: &DiscoveryAggregateWriteConfig,
    emit: &mut impl FnMut(String),
    deadline: Option<Instant>,
) -> Result<Vec<PreparedScoringSwap>> {
    if swaps.is_empty() {
        return Ok(Vec::new());
    }

    let _progress_guard =
        deadline.map(|deadline| DiscoveryScoringPrepareProgressGuard::install(conn, deadline));

    emit_prepare_stage_start(emit, "prepare_sort", None, None);
    let sort_started_at = Instant::now();
    check_prepare_runtime_budget(deadline, emit, "prepare_sort", None, None)?;
    let mut ordered = swaps.to_vec();
    ordered.sort_by(cmp_swap_order);
    emit_prepare_stage_end(
        emit,
        "prepare_sort",
        None,
        None,
        sort_started_at.elapsed().as_millis() as u64,
        "completed",
    );
    emit_prepare_batch_counts(emit, &ordered);

    let mut budget = QualityFetchBudget::default();
    let mut prepared = Vec::with_capacity(ordered.len());
    for swap in ordered {
        let buy_fact = if is_sol_buy(&swap) {
            let token = swap.token_out.as_str();
            emit_prepare_stage_start(
                emit,
                "token_market_stats",
                Some(token),
                Some(&swap.signature),
            );
            let market_stats_started_at = Instant::now();
            check_prepare_runtime_budget(
                deadline,
                emit,
                "token_market_stats",
                Some(token),
                Some(&swap.signature),
            )?;
            let market_stats = match token_market_stats_on_conn(conn, token, swap.ts_utc) {
                Ok(market_stats) => {
                    emit_prepare_stage_end(
                        emit,
                        "token_market_stats",
                        Some(token),
                        Some(&swap.signature),
                        market_stats_started_at.elapsed().as_millis() as u64,
                        "completed",
                    );
                    market_stats
                }
                Err(error) => {
                    let error = prepare_stage_error(
                        error,
                        deadline,
                        "token_market_stats",
                        Some(token),
                        Some(&swap.signature),
                    );
                    emit_prepare_stage_end(
                        emit,
                        "token_market_stats",
                        Some(token),
                        Some(&swap.signature),
                        market_stats_started_at.elapsed().as_millis() as u64,
                        if prepare_error_is_runtime_budget(&error) {
                            "runtime_budget_exhausted"
                        } else {
                            "failed"
                        },
                    );
                    return Err(error);
                }
            };
            let (quality, quality_cache_upsert) =
                resolve_quality_snapshot_on_conn_with_diagnostics(
                    conn,
                    token,
                    swap.ts_utc,
                    config,
                    &mut budget,
                    emit,
                    deadline,
                    Some(&swap.signature),
                )?;
            Some(PreparedBuyFact {
                market_stats,
                quality,
                quality_cache_upsert,
                rug_check_after_ts: swap.ts_utc
                    + Duration::seconds(config.rug_lookahead_seconds.max(1) as i64),
            })
        } else {
            None
        };
        prepared.push(PreparedScoringSwap { swap, buy_fact });
    }
    Ok(prepared)
}

fn insert_wallet_scoring_buy_fact_on_conn(
    conn: &Connection,
    swap: &SwapEvent,
    prepared: &PreparedBuyFact,
) -> Result<()> {
    let token = swap.token_out.as_str();

    conn.execute(
        "INSERT OR IGNORE INTO wallet_scoring_buy_facts(
            buy_signature,
            wallet_id,
            token,
            ts,
            activity_day,
            notional_sol,
            market_volume_5m_sol,
            market_unique_traders_5m,
            market_liquidity_proxy_sol,
            quality_source,
            quality_token_age_seconds,
            quality_holders,
            quality_liquidity_sol,
            rug_check_after_ts,
            rug_volume_lookahead_sol,
            rug_unique_traders_lookahead
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL, NULL)",
        params![
            &swap.signature,
            &swap.wallet,
            token,
            swap.ts_utc.to_rfc3339(),
            swap.ts_utc.date_naive().format("%Y-%m-%d").to_string(),
            swap.amount_in.max(0.0),
            prepared.market_stats.volume_5m_sol.max(0.0),
            prepared.market_stats.unique_traders_5m as i64,
            prepared.market_stats.liquidity_sol_proxy.max(0.0),
            match prepared.quality.source {
                WalletScoringQualitySource::Fresh => "fresh",
                WalletScoringQualitySource::Stale => "stale",
                WalletScoringQualitySource::Deferred => "deferred",
                WalletScoringQualitySource::Missing => "missing",
            },
            prepared.quality.token_age_seconds.map(|value| value as i64),
            prepared.quality.holders.map(|value| value as i64),
            prepared.quality.liquidity_sol,
            prepared.rug_check_after_ts.to_rfc3339(),
        ],
    )
    .context("failed inserting wallet_scoring_buy_facts row")?;

    insert_wallet_scoring_open_lot_on_conn(conn, swap)?;
    Ok(())
}

fn insert_wallet_scoring_open_lot_on_conn(conn: &Connection, swap: &SwapEvent) -> Result<()> {
    let token = swap.token_out.as_str();
    conn.execute(
        "INSERT OR IGNORE INTO wallet_scoring_open_lots(
            buy_signature,
            wallet_id,
            token,
            qty,
            cost_sol,
            opened_ts
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            &swap.signature,
            &swap.wallet,
            token,
            swap.amount_out.max(0.0),
            swap.amount_in.max(0.0),
            swap.ts_utc.to_rfc3339(),
        ],
    )
    .context("failed inserting wallet_scoring_open_lots row")?;
    Ok(())
}

fn load_wallet_scoring_open_lots_on_conn(
    conn: &Connection,
    wallet_id: &str,
    token: &str,
) -> Result<Vec<OpenLotRow>> {
    let mut stmt = conn
        .prepare(
            "SELECT buy_signature, qty, cost_sol, opened_ts
             FROM wallet_scoring_open_lots
             WHERE wallet_id = ?1
               AND token = ?2
             ORDER BY opened_ts ASC, buy_signature ASC",
        )
        .context("failed preparing wallet_scoring_open_lots query")?;
    let mut rows = stmt
        .query(params![wallet_id, token])
        .context("failed querying wallet_scoring_open_lots")?;
    let mut lots = Vec::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating wallet_scoring_open_lots rows")?
    {
        let opened_ts_raw: String = row
            .get(3)
            .context("failed reading wallet_scoring_open_lots.opened_ts")?;
        lots.push(OpenLotRow {
            buy_signature: row
                .get(0)
                .context("failed reading wallet_scoring_open_lots.buy_signature")?,
            qty: row
                .get(1)
                .context("failed reading wallet_scoring_open_lots.qty")?,
            cost_sol: row
                .get(2)
                .context("failed reading wallet_scoring_open_lots.cost_sol")?,
            opened_ts: parse_ts(&opened_ts_raw, "wallet_scoring_open_lots.opened_ts")?,
        });
    }
    Ok(lots)
}

fn load_wallet_scoring_carryover_lot_on_conn(
    conn: &Connection,
    wallet_id: &str,
    token: &str,
) -> Result<Option<CarryoverLotRow>> {
    let row: Option<(f64, f64, String)> = conn
        .query_row(
            "SELECT qty, cost_sol, oldest_opened_ts
             FROM wallet_scoring_carryover_lots
             WHERE wallet_id = ?1
               AND token = ?2",
            params![wallet_id, token],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .context("failed querying wallet_scoring_carryover_lots")?;
    row.map(|(qty, cost_sol, oldest_opened_ts_raw)| {
        Ok(CarryoverLotRow {
            qty,
            cost_sol,
            oldest_opened_ts: parse_ts(
                &oldest_opened_ts_raw,
                "wallet_scoring_carryover_lots.oldest_opened_ts",
            )?,
        })
    })
    .transpose()
}

fn apply_wallet_scoring_carryover_sell_on_conn(
    conn: &Connection,
    swap: &SwapEvent,
    segment_index: i64,
) -> Result<i64> {
    let token = swap.token_in.as_str();
    let Some(carryover) = load_wallet_scoring_carryover_lot_on_conn(conn, &swap.wallet, token)?
    else {
        return Ok(0);
    };
    let qty_remaining = swap.amount_in.max(0.0);
    if qty_remaining <= 1e-12 || carryover.qty <= 1e-12 {
        return Ok(0);
    }

    let take_qty = qty_remaining.min(carryover.qty);
    let lot_fraction = take_qty / carryover.qty;
    let cost_part = carryover.cost_sol * lot_fraction;
    let proceeds_part = swap.amount_out * (take_qty / swap.amount_in.max(1e-12));
    let pnl_sol = proceeds_part - cost_part;
    let remaining_qty = (carryover.qty - take_qty).max(0.0);
    let remaining_cost = (carryover.cost_sol - cost_part).max(0.0);

    if remaining_qty <= 1e-12 {
        conn.execute(
            "DELETE FROM wallet_scoring_carryover_lots
             WHERE wallet_id = ?1
               AND token = ?2",
            params![&swap.wallet, token],
        )
        .context("failed deleting consumed wallet_scoring_carryover_lot")?;
    } else {
        conn.execute(
            "UPDATE wallet_scoring_carryover_lots
             SET qty = ?3, cost_sol = ?4
             WHERE wallet_id = ?1
               AND token = ?2",
            params![&swap.wallet, token, remaining_qty, remaining_cost],
        )
        .context("failed updating partially consumed wallet_scoring_carryover_lot")?;
    }

    let hold_seconds = (swap.ts_utc - carryover.oldest_opened_ts)
        .num_seconds()
        .max(0);
    conn.execute(
        "INSERT OR IGNORE INTO wallet_scoring_close_facts(
            sell_signature,
            segment_index,
            wallet_id,
            token,
            closed_ts,
            activity_day,
            pnl_sol,
            hold_seconds,
            win
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            &swap.signature,
            segment_index,
            &swap.wallet,
            token,
            swap.ts_utc.to_rfc3339(),
            swap.ts_utc.date_naive().format("%Y-%m-%d").to_string(),
            pnl_sol,
            hold_seconds,
            if pnl_sol > 0.0 { 1 } else { 0 },
        ],
    )
    .context("failed inserting wallet_scoring_close_facts row for carryover lot")?;

    Ok(1)
}

fn apply_wallet_scoring_carryover_sell_lot_only_on_conn(
    conn: &Connection,
    swap: &SwapEvent,
) -> Result<()> {
    let token = swap.token_in.as_str();
    let Some(carryover) = load_wallet_scoring_carryover_lot_on_conn(conn, &swap.wallet, token)?
    else {
        return Ok(());
    };
    let qty_remaining = swap.amount_in.max(0.0);
    if qty_remaining <= 1e-12 || carryover.qty <= 1e-12 {
        return Ok(());
    }

    let take_qty = qty_remaining.min(carryover.qty);
    let lot_fraction = take_qty / carryover.qty;
    let cost_part = carryover.cost_sol * lot_fraction;
    let remaining_qty = (carryover.qty - take_qty).max(0.0);
    let remaining_cost = (carryover.cost_sol - cost_part).max(0.0);

    if remaining_qty <= 1e-12 {
        conn.execute(
            "DELETE FROM wallet_scoring_carryover_lots
             WHERE wallet_id = ?1
               AND token = ?2",
            params![&swap.wallet, token],
        )
        .context("failed deleting consumed wallet_scoring_carryover_lot")?;
    } else {
        conn.execute(
            "UPDATE wallet_scoring_carryover_lots
             SET qty = ?3, cost_sol = ?4
             WHERE wallet_id = ?1
               AND token = ?2",
            params![&swap.wallet, token, remaining_qty, remaining_cost],
        )
        .context("failed updating partially consumed wallet_scoring_carryover_lot")?;
    }

    Ok(())
}

fn apply_wallet_scoring_sell_on_conn(conn: &Connection, swap: &SwapEvent) -> Result<()> {
    let token = swap.token_in.as_str();
    let lots = load_wallet_scoring_open_lots_on_conn(conn, &swap.wallet, token)?;
    if lots.is_empty() {
        let _ = apply_wallet_scoring_carryover_sell_on_conn(conn, swap, 0)?;
        return Ok(());
    }

    let mut qty_remaining = swap.amount_in.max(0.0);
    if qty_remaining <= 1e-12 || swap.amount_out <= 0.0 {
        return Ok(());
    }

    let mut segment_index = 0i64;
    for lot in lots {
        if qty_remaining <= 1e-12 {
            break;
        }
        if lot.qty <= 1e-12 {
            conn.execute(
                "DELETE FROM wallet_scoring_open_lots WHERE buy_signature = ?1",
                params![&lot.buy_signature],
            )
            .context("failed deleting empty wallet_scoring_open_lot")?;
            continue;
        }

        let take_qty = qty_remaining.min(lot.qty);
        let lot_fraction = take_qty / lot.qty;
        let cost_part = lot.cost_sol * lot_fraction;
        let proceeds_part = swap.amount_out * (take_qty / swap.amount_in.max(1e-12));
        let pnl_sol = proceeds_part - cost_part;
        let remaining_qty = (lot.qty - take_qty).max(0.0);
        let remaining_cost = (lot.cost_sol - cost_part).max(0.0);

        if remaining_qty <= 1e-12 {
            conn.execute(
                "DELETE FROM wallet_scoring_open_lots WHERE buy_signature = ?1",
                params![&lot.buy_signature],
            )
            .context("failed deleting consumed wallet_scoring_open_lot")?;
        } else {
            conn.execute(
                "UPDATE wallet_scoring_open_lots
                 SET qty = ?2, cost_sol = ?3
                 WHERE buy_signature = ?1",
                params![&lot.buy_signature, remaining_qty, remaining_cost],
            )
            .context("failed updating partially consumed wallet_scoring_open_lot")?;
        }

        let hold_seconds = (swap.ts_utc - lot.opened_ts).num_seconds().max(0);
        conn.execute(
            "INSERT OR IGNORE INTO wallet_scoring_close_facts(
                sell_signature,
                segment_index,
                wallet_id,
                token,
                closed_ts,
                activity_day,
                pnl_sol,
                hold_seconds,
                win
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                &swap.signature,
                segment_index,
                &swap.wallet,
                token,
                swap.ts_utc.to_rfc3339(),
                swap.ts_utc.date_naive().format("%Y-%m-%d").to_string(),
                pnl_sol,
                hold_seconds,
                if pnl_sol > 0.0 { 1 } else { 0 },
            ],
        )
        .context("failed inserting wallet_scoring_close_facts row")?;

        qty_remaining -= take_qty;
        segment_index += 1;
    }

    if qty_remaining > 1e-12 {
        let carryover_sell = SwapEvent {
            amount_in: qty_remaining,
            ..swap.clone()
        };
        let _ = apply_wallet_scoring_carryover_sell_on_conn(conn, &carryover_sell, segment_index)?;
    }

    Ok(())
}

fn apply_wallet_scoring_sell_lot_only_on_conn(conn: &Connection, swap: &SwapEvent) -> Result<()> {
    let token = swap.token_in.as_str();
    let lots = load_wallet_scoring_open_lots_on_conn(conn, &swap.wallet, token)?;
    if lots.is_empty() {
        apply_wallet_scoring_carryover_sell_lot_only_on_conn(conn, swap)?;
        return Ok(());
    }

    let mut qty_remaining = swap.amount_in.max(0.0);
    if qty_remaining <= 1e-12 || swap.amount_out <= 0.0 {
        return Ok(());
    }

    for lot in lots {
        if qty_remaining <= 1e-12 {
            break;
        }
        if lot.qty <= 1e-12 {
            conn.execute(
                "DELETE FROM wallet_scoring_open_lots WHERE buy_signature = ?1",
                params![&lot.buy_signature],
            )
            .context("failed deleting empty wallet_scoring_open_lot")?;
            continue;
        }

        let take_qty = qty_remaining.min(lot.qty);
        let lot_fraction = take_qty / lot.qty;
        let cost_part = lot.cost_sol * lot_fraction;
        let remaining_qty = (lot.qty - take_qty).max(0.0);
        let remaining_cost = (lot.cost_sol - cost_part).max(0.0);

        if remaining_qty <= 1e-12 {
            conn.execute(
                "DELETE FROM wallet_scoring_open_lots WHERE buy_signature = ?1",
                params![&lot.buy_signature],
            )
            .context("failed deleting consumed wallet_scoring_open_lot")?;
        } else {
            conn.execute(
                "UPDATE wallet_scoring_open_lots
                 SET qty = ?2, cost_sol = ?3
                 WHERE buy_signature = ?1",
                params![&lot.buy_signature, remaining_qty, remaining_cost],
            )
            .context("failed updating partially consumed wallet_scoring_open_lot")?;
        }

        qty_remaining -= take_qty;
    }

    if qty_remaining > 1e-12 {
        let carryover_sell = SwapEvent {
            amount_in: qty_remaining,
            ..swap.clone()
        };
        apply_wallet_scoring_carryover_sell_lot_only_on_conn(conn, &carryover_sell)?;
    }

    Ok(())
}

fn rug_lookahead_stats_on_conn(
    conn: &Connection,
    token: &str,
    buy_ts: DateTime<Utc>,
    lookahead_end: DateTime<Utc>,
) -> Result<(f64, u32)> {
    if rug_lookahead_unknown_failpoint_triggered() {
        return Err(anyhow!(
            "test failpoint: discovery scoring rug lookahead unknown failure"
        ))
        .context("failed querying discovery scoring rug lookahead stats");
    }
    if rug_lookahead_budget_failpoint_triggered() {
        return Err(anyhow!(
            DISCOVERY_AGGREGATE_REPAIR_LOCK_FIRST_BUDGET_EXHAUSTED_WITHOUT_PROGRESS
        ))
        .context("failed querying discovery scoring rug lookahead stats");
    }
    let (volume_sol, unique_traders_raw): (f64, i64) = conn
        .query_row(
            RUG_LOOKAHEAD_STATS_QUERY,
            params![
                token,
                SOL_MINT,
                buy_ts.to_rfc3339(),
                lookahead_end.to_rfc3339(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .context("failed querying discovery scoring rug lookahead stats")?;
    Ok((volume_sol.max(0.0), unique_traders_raw.max(0) as u32))
}

fn finalize_mature_rug_facts_on_conn(conn: &Connection, watermark_ts: DateTime<Utc>) -> Result<()> {
    let mut stmt = conn
        .prepare(
            "SELECT buy_signature, token, ts, rug_check_after_ts
             FROM wallet_scoring_buy_facts
             WHERE rug_volume_lookahead_sol IS NULL
               AND rug_check_after_ts <= ?1
             ORDER BY rug_check_after_ts ASC, ts ASC",
        )
        .context("failed preparing pending rug facts query")?;
    let mut rows = stmt
        .query(params![watermark_ts.to_rfc3339()])
        .context("failed querying pending rug facts")?;
    let mut pending = Vec::new();
    while let Some(row) = rows.next().context("failed iterating pending rug facts")? {
        let buy_ts_raw: String = row
            .get(2)
            .context("failed reading wallet_scoring_buy_facts.ts")?;
        let check_after_raw: String = row
            .get(3)
            .context("failed reading wallet_scoring_buy_facts.rug_check_after_ts")?;
        pending.push((
            row.get::<_, String>(0)
                .context("failed reading wallet_scoring_buy_facts.buy_signature")?,
            row.get::<_, String>(1)
                .context("failed reading wallet_scoring_buy_facts.token")?,
            parse_ts(&buy_ts_raw, "wallet_scoring_buy_facts.ts")?,
            parse_ts(
                &check_after_raw,
                "wallet_scoring_buy_facts.rug_check_after_ts",
            )?,
        ));
    }

    for (buy_signature, token, buy_ts, check_after_ts) in pending {
        let (volume_sol, unique_traders) =
            rug_lookahead_stats_on_conn(conn, &token, buy_ts, check_after_ts)?;
        conn.execute(
            "UPDATE wallet_scoring_buy_facts
             SET rug_volume_lookahead_sol = ?2,
                 rug_unique_traders_lookahead = ?3
             WHERE buy_signature = ?1",
            params![buy_signature, volume_sol, unique_traders as i64],
        )
        .context("failed updating matured rug facts")?;
    }
    Ok(())
}

fn apply_discovery_scoring_swaps_on_conn(
    conn: &Connection,
    prepared_swaps: &[PreparedScoringSwap],
) -> Result<()> {
    for prepared in prepared_swaps {
        let swap = &prepared.swap;
        upsert_wallet_scoring_day_on_conn(conn, swap)?;
        upsert_wallet_scoring_tx_minute_on_conn(
            conn,
            &swap.wallet,
            swap.ts_utc.timestamp().div_euclid(60),
        )?;
        if let Some(buy_fact) = prepared.buy_fact.as_ref() {
            if let Some(cache_upsert) = buy_fact.quality_cache_upsert.as_ref() {
                upsert_token_quality_cache_on_conn(
                    conn,
                    &cache_upsert.mint,
                    cache_upsert.holders,
                    cache_upsert.liquidity_sol,
                    cache_upsert.token_age_seconds,
                    cache_upsert.fetched_at,
                )?;
            }
            insert_wallet_scoring_buy_fact_on_conn(conn, swap, buy_fact)?;
        } else if is_sol_sell(swap) {
            apply_wallet_scoring_sell_on_conn(conn, swap)?;
        }
    }
    Ok(())
}

fn apply_discovery_scoring_boundary_lot_swaps_on_conn(
    conn: &Connection,
    swaps: &[SwapEvent],
) -> Result<()> {
    for swap in swaps {
        if is_sol_buy(swap) {
            insert_wallet_scoring_open_lot_on_conn(conn, swap)?;
        } else if is_sol_sell(swap) {
            apply_wallet_scoring_sell_lot_only_on_conn(conn, swap)?;
        }
    }
    Ok(())
}

fn apply_discovery_scoring_swaps_and_checkpoint_on_conn(
    conn: &Connection,
    prepared_swaps: &[PreparedScoringSwap],
    progress_start_ts: DateTime<Utc>,
    progress_cursor: &DiscoveryRuntimeCursor,
    stage_start: &mut impl FnMut(&str),
    stage_end: &mut impl FnMut(&str, usize, u64, &str),
) -> Result<(u64, u64)> {
    stage_start("aggregate_batch_apply");
    let apply_started_at = Instant::now();
    if let Err(error) = apply_discovery_scoring_swaps_on_conn(conn, prepared_swaps) {
        stage_end(
            "aggregate_batch_apply",
            prepared_swaps.len(),
            apply_started_at.elapsed().as_millis() as u64,
            "failed",
        );
        return Err(error);
    }
    let apply_ms = apply_started_at.elapsed().as_millis() as u64;
    stage_end(
        "aggregate_batch_apply",
        prepared_swaps.len(),
        apply_ms,
        "completed",
    );

    maybe_fail_after_materialization_before_checkpoint()?;

    stage_start("progress_checkpoint");
    let progress_started_at = Instant::now();
    let updated_at = Utc::now().to_rfc3339();
    if let Err(error) = upsert_discovery_scoring_backfill_progress_on_conn(
        conn,
        progress_start_ts,
        progress_cursor,
        &updated_at,
    ) {
        stage_end(
            "progress_checkpoint",
            prepared_swaps.len(),
            progress_started_at.elapsed().as_millis() as u64,
            "failed",
        );
        return Err(error);
    }
    let progress_update_ms = progress_started_at.elapsed().as_millis() as u64;
    stage_end(
        "progress_checkpoint",
        prepared_swaps.len(),
        progress_update_ms,
        "completed",
    );

    Ok((apply_ms, progress_update_ms))
}

fn apply_discovery_scoring_boundary_lot_swaps_and_checkpoint_on_conn(
    conn: &Connection,
    swaps: &[SwapEvent],
    progress_start_ts: DateTime<Utc>,
    progress_cursor: &DiscoveryRuntimeCursor,
    stage_start: &mut impl FnMut(&str),
    stage_end: &mut impl FnMut(&str, usize, u64, &str),
) -> Result<(u64, u64)> {
    stage_start("aggregate_batch_apply");
    let apply_started_at = Instant::now();
    if let Err(error) = apply_discovery_scoring_boundary_lot_swaps_on_conn(conn, swaps) {
        stage_end(
            "aggregate_batch_apply",
            swaps.len(),
            apply_started_at.elapsed().as_millis() as u64,
            "failed",
        );
        return Err(error);
    }
    let apply_ms = apply_started_at.elapsed().as_millis() as u64;
    stage_end("aggregate_batch_apply", swaps.len(), apply_ms, "completed");

    maybe_fail_after_materialization_before_checkpoint()?;

    stage_start("progress_checkpoint");
    let progress_started_at = Instant::now();
    let updated_at = Utc::now().to_rfc3339();
    if let Err(error) = upsert_discovery_scoring_backfill_progress_on_conn(
        conn,
        progress_start_ts,
        progress_cursor,
        &updated_at,
    ) {
        stage_end(
            "progress_checkpoint",
            swaps.len(),
            progress_started_at.elapsed().as_millis() as u64,
            "failed",
        );
        return Err(error);
    }
    let progress_update_ms = progress_started_at.elapsed().as_millis() as u64;
    stage_end(
        "progress_checkpoint",
        swaps.len(),
        progress_update_ms,
        "completed",
    );

    Ok((apply_ms, progress_update_ms))
}

impl SqliteStore {
    fn upsert_discovery_scoring_state_ts(
        &self,
        state_key: &str,
        value: DateTime<Utc>,
    ) -> Result<()> {
        self.with_immediate_transaction_retry("discovery scoring state update", |conn| {
            conn.execute(
                "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                params![state_key, value.to_rfc3339(), Utc::now().to_rfc3339()],
            )
            .with_context(|| format!("failed upserting discovery_scoring_state.{state_key}"))?;
            Ok(0usize)
        })?;
        Ok(())
    }

    fn load_discovery_scoring_state_ts(&self, state_key: &str) -> Result<Option<DateTime<Utc>>> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = ?1",
                params![state_key],
                |row| row.get(0),
            )
            .optional()
            .with_context(|| format!("failed querying discovery_scoring_state.{state_key}"))?;
        raw.map(|raw| parse_ts(&raw, &format!("discovery_scoring_state.{state_key}")))
            .transpose()
    }

    fn load_discovery_scoring_state_value(&self, state_key: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = ?1",
                params![state_key],
                |row| row.get(0),
            )
            .optional()
            .with_context(|| format!("failed querying discovery_scoring_state.{state_key}"))
    }

    fn load_discovery_scoring_cursor_state_exact(
        &self,
        ts_key: &str,
        slot_key: &str,
        signature_key: &str,
        label: &str,
    ) -> Result<Option<DiscoveryRuntimeCursor>> {
        let ts_utc = self.load_discovery_scoring_state_ts(ts_key)?;
        let slot_raw = self.load_discovery_scoring_state_value(slot_key)?;
        let signature = self.load_discovery_scoring_state_value(signature_key)?;
        match (ts_utc, slot_raw, signature) {
            (None, None, None) => Ok(None),
            (Some(ts_utc), Some(slot_raw), Some(signature)) => {
                let slot = slot_raw.parse::<u64>().with_context(|| {
                    format!("invalid discovery_scoring_state.{slot_key} value: {slot_raw}")
                })?;
                Ok(Some(DiscoveryRuntimeCursor {
                    ts_utc,
                    slot,
                    signature,
                }))
            }
            _ => Err(anyhow!(
                "discovery_scoring_state.{label} cursor is partially populated"
            )),
        }
    }

    pub fn apply_discovery_scoring_batch(
        &self,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
    ) -> Result<()> {
        let prepared = prepare_discovery_scoring_swaps(&self.conn, swaps, config)?;
        self.with_immediate_transaction_retry("discovery scoring batch", |conn| {
            apply_discovery_scoring_swaps_on_conn(conn, &prepared)
        })
    }

    pub fn apply_discovery_scoring_repair_commit_group(
        &self,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
        expected_start_cursor: &DiscoveryRuntimeCursor,
        covered_through_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<&'static str> {
        let prepared = prepare_discovery_scoring_swaps(&self.conn, swaps, config)?;
        self.with_immediate_transaction_retry(
            "discovery scoring aggregate repair commit group",
            |conn| {
                let current = load_discovery_scoring_cursor_state_exact_on_conn(
                    conn,
                    "covered_through_ts",
                    "covered_through_slot",
                    "covered_through_signature",
                    "covered_through",
                )?;
                let Some(current) = current else {
                    anyhow::bail!(
                        "discovery scoring covered_through cursor missing before repair commit group"
                    );
                };
                match cmp_cursor_order(&current, expected_start_cursor) {
                    Ordering::Equal => {}
                    Ordering::Greater => {
                        if cmp_cursor_order(&current, covered_through_cursor) != Ordering::Less {
                            return Ok("already_covered");
                        }
                        return Ok("concurrent_progress");
                    }
                    Ordering::Less => {
                        anyhow::bail!(
                            "discovery scoring covered_through cursor is before expected repair start cursor"
                        );
                    }
                }

                apply_discovery_scoring_swaps_on_conn(conn, &prepared)?;
                finalize_mature_rug_facts_on_conn(conn, covered_through_cursor.ts_utc)?;
                let now = Utc::now().to_rfc3339();
                upsert_discovery_scoring_cursor_state_on_conn(
                    conn,
                    "covered_through_ts",
                    "covered_through_slot",
                    "covered_through_signature",
                    covered_through_cursor,
                    &now,
                )?;
                Ok("committed")
            },
        )
    }

    pub fn apply_discovery_scoring_repair_micro_commit_lock_first(
        &self,
        config: &DiscoveryAggregateWriteConfig,
        expected_gap_cursor: &DiscoveryRuntimeCursor,
        expected_repair_target: &DiscoveryRuntimeCursor,
        max_rows: usize,
        max_lock_duration: StdDuration,
    ) -> Result<(
        &'static str,
        Option<DiscoveryRuntimeCursor>,
        Option<DiscoveryRuntimeCursor>,
        usize,
        bool,
        bool,
    )> {
        self.with_immediate_transaction_retry(
            "discovery scoring aggregate repair lock-first micro commit",
            |conn| {
                let lock_started = Instant::now();
                let lock_deadline = lock_started
                    .checked_add(max_lock_duration)
                    .unwrap_or(lock_started);
                let collect_budget_ms = ((max_lock_duration.as_millis().saturating_mul(4)) / 5)
                    .max(1)
                    .min(u128::from(u64::MAX)) as u64;
                let collect_deadline = lock_started
                    .checked_add(StdDuration::from_millis(collect_budget_ms))
                    .unwrap_or(lock_deadline);
                check_lock_first_repair_deadline(lock_deadline)?;
                let current = load_discovery_scoring_cursor_state_exact_on_conn(
                    conn,
                    "covered_through_ts",
                    "covered_through_slot",
                    "covered_through_signature",
                    "covered_through",
                )?;
                let Some(current) = current else {
                    return Ok(("covered_missing", None, None, 0, false, false));
                };

                let Some(gap_cursor) =
                    load_discovery_scoring_materialization_gap_cursor_on_conn(conn)?
                else {
                    return Ok((
                        "gap_missing",
                        Some(current.clone()),
                        Some(current),
                        0,
                        false,
                        false,
                    ));
                };
                if cmp_cursor_order(&gap_cursor, expected_gap_cursor) != Ordering::Equal {
                    return Ok((
                        "state_mismatch",
                        Some(current.clone()),
                        Some(current),
                        0,
                        false,
                        false,
                    ));
                }

                let Some((target_gap_cursor, repair_target)) =
                    load_discovery_scoring_materialization_gap_repair_target_on_conn(conn)?
                else {
                    return Ok((
                        "target_missing",
                        Some(current.clone()),
                        Some(current),
                        0,
                        false,
                        false,
                    ));
                };
                if cmp_cursor_order(&target_gap_cursor, expected_gap_cursor) != Ordering::Equal
                    || cmp_cursor_order(&repair_target, expected_repair_target) != Ordering::Equal
                {
                    return Ok((
                        "state_mismatch",
                        Some(current.clone()),
                        Some(current),
                        0,
                        false,
                        false,
                    ));
                }

                let exact_gap_exists =
                    observed_swap_exact_cursor_exists_on_conn(conn, &gap_cursor)?;
                if !exact_gap_exists {
                    return Ok((
                        "exact_gap_missing",
                        Some(current.clone()),
                        Some(current),
                        0,
                        false,
                        false,
                    ));
                }

                let gap_observed_before = cmp_cursor_order(&current, &gap_cursor) != Ordering::Less;
                if cmp_cursor_order(&current, &repair_target) != Ordering::Less {
                    return Ok((
                        "already_at_target",
                        Some(current.clone()),
                        Some(current),
                        0,
                        true,
                        gap_observed_before,
                    ));
                }

                let mut swaps = Vec::with_capacity(max_rows.min(1_024));
                let mut page_cursor = current.clone();
                let mut reached_target = false;
                let mut collection_budget_exhausted = false;
                {
                    let _query_progress_guard =
                        DiscoveryScoringPrepareProgressGuard::install(conn, lock_deadline);
                    while swaps.len() < max_rows && !reached_target {
                        if lock_first_repair_budget_reached(swaps.len())
                            || Instant::now() >= collect_deadline
                        {
                            collection_budget_exhausted = true;
                            break;
                        }
                        let mut page_limit = max_rows
                            .saturating_sub(swaps.len())
                            .min(DISCOVERY_SCORING_LOCK_FIRST_REPAIR_QUERY_PAGE_ROWS);
                        if let Some(test_budget_rows) =
                            lock_first_repair_budget_after_rows_for_tests()
                        {
                            page_limit =
                                page_limit.min(test_budget_rows.saturating_sub(swaps.len()));
                        }
                        if page_limit == 0 {
                            break;
                        }
                        let (page, page_reached_target) =
                            load_observed_swaps_after_cursor_for_repair_on_conn(
                                conn,
                                &page_cursor,
                                &repair_target,
                                page_limit,
                            )?;
                        if page.is_empty() {
                            break;
                        }
                        page_cursor = DiscoveryRuntimeCursor {
                            ts_utc: page.last().expect("non-empty repair page").ts_utc,
                            slot: page.last().expect("non-empty repair page").slot,
                            signature: page
                                .last()
                                .expect("non-empty repair page")
                                .signature
                                .clone(),
                        };
                        reached_target |= page_reached_target;
                        swaps.extend(page);
                    }
                }
                if swaps.is_empty() {
                    if collection_budget_exhausted || Instant::now() >= collect_deadline {
                        return Ok((
                            "budget_exhausted_without_progress",
                            Some(current.clone()),
                            Some(current),
                            0,
                            false,
                            gap_observed_before,
                        ));
                    }
                    return Ok((
                        "no_rows",
                        Some(current.clone()),
                        Some(current),
                        0,
                        false,
                        gap_observed_before,
                    ));
                }
                let last_cursor = DiscoveryRuntimeCursor {
                    ts_utc: swaps.last().expect("non-empty repair swaps").ts_utc,
                    slot: swaps.last().expect("non-empty repair swaps").slot,
                    signature: swaps
                        .last()
                        .expect("non-empty repair swaps")
                        .signature
                        .clone(),
                };
                let gap_observed = gap_observed_before
                    || swaps.iter().any(|swap| {
                        cmp_cursor_order(
                            &DiscoveryRuntimeCursor {
                                ts_utc: swap.ts_utc,
                                slot: swap.slot,
                                signature: swap.signature.clone(),
                            },
                            &gap_cursor,
                        ) == Ordering::Equal
                    });

                if Instant::now() >= lock_deadline {
                    return Ok((
                        "budget_exhausted_without_progress",
                        Some(current.clone()),
                        Some(current),
                        0,
                        false,
                        gap_observed_before,
                    ));
                }
                let prepared = prepare_discovery_scoring_swaps_with_diagnostics(
                    conn,
                    &swaps,
                    config,
                    &mut |_| {},
                    Some(lock_deadline),
                )?;
                check_lock_first_repair_deadline(lock_deadline)?;
                let _write_progress_guard =
                    DiscoveryScoringPrepareProgressGuard::install(conn, lock_deadline);
                apply_discovery_scoring_swaps_on_conn(conn, &prepared)?;
                check_lock_first_repair_deadline(lock_deadline)?;
                let _repair_rows_guard = LockFirstRepairCurrentRowsGuard::install(swaps.len());
                finalize_mature_rug_facts_on_conn(conn, last_cursor.ts_utc)?;
                check_lock_first_repair_deadline(lock_deadline)?;
                let now = Utc::now().to_rfc3339();
                upsert_discovery_scoring_cursor_state_on_conn(
                    conn,
                    "covered_through_ts",
                    "covered_through_slot",
                    "covered_through_signature",
                    &last_cursor,
                    &now,
                )?;
                Ok((
                    "committed",
                    Some(current),
                    Some(last_cursor),
                    swaps.len(),
                    reached_target,
                    gap_observed,
                ))
            },
        )
    }

    pub fn apply_discovery_scoring_batch_with_timings(
        &self,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
    ) -> Result<DiscoveryScoringBatchStageTimings> {
        let prepare_started_at = Instant::now();
        let prepared = prepare_discovery_scoring_swaps(&self.conn, swaps, config)?;
        let prepare_ms = prepare_started_at.elapsed().as_millis() as u64;

        let apply_started_at = Instant::now();
        self.with_immediate_transaction_retry("discovery scoring batch", |conn| {
            apply_discovery_scoring_swaps_on_conn(conn, &prepared)
        })?;
        let apply_ms = apply_started_at.elapsed().as_millis() as u64;

        Ok(DiscoveryScoringBatchStageTimings {
            prepare_ms,
            apply_ms,
            rug_finalize_ms: 0,
        })
    }

    pub fn apply_discovery_scoring_batch_and_checkpoint_with_timings(
        &self,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<super::DiscoveryScoringCheckpointedBatchTimings> {
        self.apply_discovery_scoring_batch_and_checkpoint_with_timings_and_diagnostics(
            swaps,
            config,
            progress_start_ts,
            progress_cursor,
            |_| {},
            |_, _, _, _| {},
            |_| {},
            None,
        )
    }

    pub fn apply_discovery_scoring_batch_and_checkpoint_with_timings_and_diagnostics(
        &self,
        swaps: &[SwapEvent],
        config: &DiscoveryAggregateWriteConfig,
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
        mut stage_start: impl FnMut(&str),
        mut stage_end: impl FnMut(&str, usize, u64, &str),
        mut prepare_event: impl FnMut(String),
        prepare_deadline: Option<Instant>,
    ) -> Result<super::DiscoveryScoringCheckpointedBatchTimings> {
        stage_start("prepare_discovery_scoring_swaps");
        let prepare_started_at = Instant::now();
        let prepared = match prepare_discovery_scoring_swaps_with_diagnostics(
            &self.conn,
            swaps,
            config,
            &mut prepare_event,
            prepare_deadline,
        ) {
            Ok(prepared) => {
                stage_end(
                    "prepare_discovery_scoring_swaps",
                    prepared.len(),
                    prepare_started_at.elapsed().as_millis() as u64,
                    "completed",
                );
                prepared
            }
            Err(error) => {
                let outcome = if prepare_error_is_runtime_budget(&error) {
                    "runtime_budget_exhausted"
                } else {
                    "failed"
                };
                stage_end(
                    "prepare_discovery_scoring_swaps",
                    swaps.len(),
                    prepare_started_at.elapsed().as_millis() as u64,
                    outcome,
                );
                return Err(error);
            }
        };
        let prepare_ms = prepare_started_at.elapsed().as_millis() as u64;

        let (apply_ms, progress_update_ms) = self.with_immediate_transaction_retry(
            "discovery scoring batch with checkpoint",
            |conn| {
                apply_discovery_scoring_swaps_and_checkpoint_on_conn(
                    conn,
                    &prepared,
                    progress_start_ts,
                    progress_cursor,
                    &mut stage_start,
                    &mut stage_end,
                )
            },
        )?;

        Ok(super::DiscoveryScoringCheckpointedBatchTimings {
            prepare_ms,
            apply_ms,
            progress_update_ms,
        })
    }

    pub fn apply_discovery_scoring_boundary_lot_batch_and_checkpoint_with_timings(
        &self,
        swaps: &[SwapEvent],
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<super::DiscoveryScoringCheckpointedBatchTimings> {
        self.apply_discovery_scoring_boundary_lot_batch_and_checkpoint_with_timings_and_diagnostics(
            swaps,
            progress_start_ts,
            progress_cursor,
            |_| {},
            |_, _, _, _| {},
        )
    }

    pub fn apply_discovery_scoring_boundary_lot_batch_and_checkpoint_with_timings_and_diagnostics(
        &self,
        swaps: &[SwapEvent],
        progress_start_ts: DateTime<Utc>,
        progress_cursor: &DiscoveryRuntimeCursor,
        mut stage_start: impl FnMut(&str),
        mut stage_end: impl FnMut(&str, usize, u64, &str),
    ) -> Result<super::DiscoveryScoringCheckpointedBatchTimings> {
        let (apply_ms, progress_update_ms) = self.with_immediate_transaction_retry(
            "discovery scoring boundary lot sql batch with checkpoint",
            |conn| {
                apply_discovery_scoring_boundary_lot_swaps_and_checkpoint_on_conn(
                    conn,
                    swaps,
                    progress_start_ts,
                    progress_cursor,
                    &mut stage_start,
                    &mut stage_end,
                )
            },
        )?;

        Ok(super::DiscoveryScoringCheckpointedBatchTimings {
            prepare_ms: 0,
            apply_ms,
            progress_update_ms,
        })
    }

    pub fn finalize_discovery_scoring_rug_facts(&self, watermark_ts: DateTime<Utc>) -> Result<()> {
        self.with_immediate_transaction_retry("discovery scoring rug finalize", |conn| {
            finalize_mature_rug_facts_on_conn(conn, watermark_ts)?;
            Ok(0usize)
        })?;
        Ok(())
    }

    pub fn finalize_discovery_scoring_rug_facts_with_timing(
        &self,
        watermark_ts: DateTime<Utc>,
    ) -> Result<u64> {
        let started_at = Instant::now();
        self.finalize_discovery_scoring_rug_facts(watermark_ts)?;
        Ok(started_at.elapsed().as_millis() as u64)
    }

    pub fn export_discovery_scoring_boundary_seed_snapshot(
        &self,
        boundary_start_ts: DateTime<Utc>,
        boundary_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<DiscoveryScoringBoundarySeedSnapshot> {
        let carryover_lot_count = wallet_scoring_carryover_lot_count_on_conn(&self.conn)?;
        if carryover_lot_count != 0 {
            anyhow::bail!(
                "exact seeded boundary export does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_lot_count}"
            );
        }
        Ok(DiscoveryScoringBoundarySeedSnapshot {
            boundary_start_ts,
            boundary_cursor: boundary_cursor.clone(),
            open_lots: load_wallet_scoring_boundary_seed_lots_on_conn(&self.conn)?,
        })
    }

    pub fn count_discovery_scoring_boundary_seed_open_lots(&self) -> Result<usize> {
        let carryover_lot_count = wallet_scoring_carryover_lot_count_on_conn(&self.conn)?;
        if carryover_lot_count != 0 {
            anyhow::bail!(
                "exact seeded boundary export does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_lot_count}"
            );
        }
        wallet_scoring_open_lot_count_on_conn(&self.conn)
    }

    pub fn reset_discovery_scoring_tables_and_install_boundary_seed_snapshot(
        &self,
        snapshot: &DiscoveryScoringBoundarySeedSnapshot,
        preserved_gap_cursor: Option<&DiscoveryRuntimeCursor>,
    ) -> Result<()> {
        self.with_immediate_transaction_retry("discovery scoring seeded reset install", |conn| {
            let carryover_lot_count = wallet_scoring_carryover_lot_count_on_conn(conn)?;
            if carryover_lot_count != 0 {
                anyhow::bail!(
                    "exact seeded boundary install does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_lot_count}"
                );
            }

            conn.execute("DELETE FROM wallet_scoring_buy_facts", [])
                .context("failed clearing wallet_scoring_buy_facts")?;
            conn.execute("DELETE FROM wallet_scoring_close_facts", [])
                .context("failed clearing wallet_scoring_close_facts")?;
            conn.execute("DELETE FROM wallet_scoring_open_lots", [])
                .context("failed clearing wallet_scoring_open_lots")?;
            conn.execute("DELETE FROM wallet_scoring_carryover_lots", [])
                .context("failed clearing wallet_scoring_carryover_lots")?;
            conn.execute("DELETE FROM wallet_scoring_tx_minutes", [])
                .context("failed clearing wallet_scoring_tx_minutes")?;
            conn.execute("DELETE FROM wallet_scoring_days", [])
                .context("failed clearing wallet_scoring_days")?;
            conn.execute("DELETE FROM discovery_scoring_state", [])
                .context("failed clearing discovery_scoring_state")?;

            for lot in &snapshot.open_lots {
                conn.execute(
                    "INSERT OR IGNORE INTO wallet_scoring_open_lots(
                        buy_signature,
                        wallet_id,
                        token,
                        qty,
                        cost_sol,
                        opened_ts
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        &lot.buy_signature,
                        &lot.wallet_id,
                        &lot.token,
                        lot.qty,
                        lot.cost_sol,
                        lot.opened_ts.to_rfc3339(),
                    ],
                )
                .context("failed inserting discovery scoring boundary seed open_lot")?;
            }

            let updated_at = Utc::now().to_rfc3339();
            upsert_discovery_scoring_backfill_progress_on_conn(
                conn,
                snapshot.boundary_start_ts,
                &snapshot.boundary_cursor,
                &updated_at,
            )?;
            upsert_discovery_scoring_seed_boundary_install_marker_on_conn(
                conn,
                snapshot.boundary_start_ts,
                &snapshot.boundary_cursor,
                &updated_at,
            )?;
            if let Some(gap_cursor) = preserved_gap_cursor {
                upsert_discovery_scoring_materialization_gap_cursor_on_conn(
                    conn,
                    gap_cursor,
                    &updated_at,
                )?;
            }
            Ok(snapshot.open_lots.len())
        })?;
        Ok(())
    }

    pub fn reset_discovery_scoring_tables_and_commit_existing_boundary_open_lots(
        &self,
        boundary_start_ts: DateTime<Utc>,
        boundary_cursor: &DiscoveryRuntimeCursor,
        preserved_gap_cursor: Option<&DiscoveryRuntimeCursor>,
    ) -> Result<usize> {
        self.with_immediate_transaction_retry("discovery scoring seeded reset install", |conn| {
            let carryover_lot_count = wallet_scoring_carryover_lot_count_on_conn(conn)?;
            if carryover_lot_count != 0 {
                anyhow::bail!(
                    "exact seeded boundary install does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_lot_count}"
                );
            }
            let open_lot_count = wallet_scoring_open_lot_count_on_conn(conn)?;

            conn.execute("DELETE FROM wallet_scoring_buy_facts", [])
                .context("failed clearing wallet_scoring_buy_facts")?;
            conn.execute("DELETE FROM wallet_scoring_close_facts", [])
                .context("failed clearing wallet_scoring_close_facts")?;
            conn.execute("DELETE FROM wallet_scoring_carryover_lots", [])
                .context("failed clearing wallet_scoring_carryover_lots")?;
            conn.execute("DELETE FROM wallet_scoring_tx_minutes", [])
                .context("failed clearing wallet_scoring_tx_minutes")?;
            conn.execute("DELETE FROM wallet_scoring_days", [])
                .context("failed clearing wallet_scoring_days")?;
            conn.execute("DELETE FROM discovery_scoring_state", [])
                .context("failed clearing discovery_scoring_state")?;

            let updated_at = Utc::now().to_rfc3339();
            upsert_discovery_scoring_backfill_progress_on_conn(
                conn,
                boundary_start_ts,
                boundary_cursor,
                &updated_at,
            )?;
            upsert_discovery_scoring_seed_boundary_install_marker_on_conn(
                conn,
                boundary_start_ts,
                boundary_cursor,
                &updated_at,
            )?;
            if let Some(gap_cursor) = preserved_gap_cursor {
                upsert_discovery_scoring_materialization_gap_cursor_on_conn(
                    conn,
                    gap_cursor,
                    &updated_at,
                )?;
            }
            Ok(open_lot_count)
        })
    }

    pub fn reset_discovery_scoring_tables(&self) -> Result<()> {
        self.with_immediate_transaction_retry("discovery scoring reset", |conn| {
            conn.execute("DELETE FROM wallet_scoring_buy_facts", [])
                .context("failed clearing wallet_scoring_buy_facts")?;
            conn.execute("DELETE FROM wallet_scoring_close_facts", [])
                .context("failed clearing wallet_scoring_close_facts")?;
            conn.execute("DELETE FROM wallet_scoring_open_lots", [])
                .context("failed clearing wallet_scoring_open_lots")?;
            conn.execute("DELETE FROM wallet_scoring_carryover_lots", [])
                .context("failed clearing wallet_scoring_carryover_lots")?;
            conn.execute("DELETE FROM wallet_scoring_tx_minutes", [])
                .context("failed clearing wallet_scoring_tx_minutes")?;
            conn.execute("DELETE FROM wallet_scoring_days", [])
                .context("failed clearing wallet_scoring_days")?;
            conn.execute("DELETE FROM discovery_scoring_state", [])
                .context("failed clearing discovery_scoring_state")?;
            Ok(0usize)
        })?;
        Ok(())
    }

    pub fn set_discovery_scoring_covered_since(&self, covered_since: DateTime<Utc>) -> Result<()> {
        self.upsert_discovery_scoring_state_ts("covered_since_ts", covered_since)
    }

    pub fn set_discovery_scoring_backfill_progress(
        &self,
        start_ts: DateTime<Utc>,
        cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring backfill progress update",
            |conn| {
                let now = Utc::now().to_rfc3339();
                clear_discovery_scoring_seed_boundary_install_marker_on_conn(conn)?;
                upsert_discovery_scoring_backfill_progress_on_conn(conn, start_ts, cursor, &now)?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn set_discovery_scoring_materialization_gap_cursor(
        &self,
        cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        let existing = self.load_discovery_scoring_materialization_gap_cursor()?;
        if existing
            .as_ref()
            .is_some_and(|current| cmp_cursor_order(current, cursor) != Ordering::Greater)
        {
            return Ok(());
        }
        self.with_immediate_transaction_retry(
            "discovery scoring materialization gap update",
            |conn| {
                let now = Utc::now().to_rfc3339();
                clear_discovery_scoring_materialization_gap_repair_target_on_conn(conn)?;
                upsert_discovery_scoring_materialization_gap_cursor_on_conn(conn, cursor, &now)?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn set_discovery_scoring_materialization_gap_repair_target(
        &self,
        gap_cursor: &DiscoveryRuntimeCursor,
        target_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring materialization gap repair target update",
            |conn| {
                let now = Utc::now().to_rfc3339();
                upsert_discovery_scoring_materialization_gap_repair_target_on_conn(
                    conn,
                    gap_cursor,
                    target_cursor,
                    &now,
                )?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn clear_discovery_scoring_materialization_gap_repair_target(&self) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring materialization gap repair target clear",
            |conn| {
                clear_discovery_scoring_materialization_gap_repair_target_on_conn(conn)?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn set_discovery_scoring_covered_through_cursor(
        &self,
        cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        let existing = self.load_discovery_scoring_covered_through_cursor()?;
        if existing
            .as_ref()
            .is_some_and(|current| cmp_cursor_order(current, cursor) != Ordering::Less)
        {
            return Ok(());
        }
        self.with_immediate_transaction_retry(
            "discovery scoring covered_through cursor update",
            |conn| {
                let now = Utc::now().to_rfc3339();
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('covered_through_ts', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![cursor.ts_utc.to_rfc3339(), &now],
                )
                .context("failed upserting discovery_scoring_state.covered_through_ts")?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('covered_through_slot', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![cursor.slot.to_string(), &now],
                )
                .context("failed upserting discovery_scoring_state.covered_through_slot")?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('covered_through_signature', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![&cursor.signature, &now],
                )
                .context("failed upserting discovery_scoring_state.covered_through_signature")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn clear_discovery_scoring_materialization_gap_if_cursor_observed(
        &self,
        observed_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        let Some(gap_cursor) = self.load_discovery_scoring_materialization_gap_cursor()? else {
            return Ok(());
        };
        if cmp_cursor_order(observed_cursor, &gap_cursor) != Ordering::Equal {
            return Ok(());
        }
        self.with_immediate_transaction_retry(
            "discovery scoring materialization gap clear",
            |conn| {
                conn.execute(
                    "DELETE FROM discovery_scoring_state
                 WHERE state_key IN (
                    'materialization_gap_since_ts',
                    'materialization_gap_since_slot',
                    'materialization_gap_since_signature',
                    'materialization_gap_repair_gap_ts',
                    'materialization_gap_repair_gap_slot',
                    'materialization_gap_repair_gap_signature',
                    'materialization_gap_repair_target_ts',
                    'materialization_gap_repair_target_slot',
                    'materialization_gap_repair_target_signature'
                 )",
                    [],
                )
                .context("failed clearing discovery_scoring_state.materialization_gap cursor")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn set_discovery_scoring_backfill_source_protection(
        &self,
        protect_since: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring backfill source protection",
            |conn| {
                let now = Utc::now().to_rfc3339();
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('backfill_protect_since_ts', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![protect_since.to_rfc3339(), &now],
                )
                .context("failed upserting discovery_scoring_state.backfill_protect_since_ts")?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('backfill_protect_expires_at', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![expires_at.to_rfc3339(), &now],
                )
                .context("failed upserting discovery_scoring_state.backfill_protect_expires_at")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn clear_discovery_scoring_backfill_source_protection(&self) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring backfill source protection clear",
            |conn| {
                conn.execute(
                    "DELETE FROM discovery_scoring_state
                 WHERE state_key IN ('backfill_protect_since_ts', 'backfill_protect_expires_at')",
                    [],
                )
                .context("failed clearing discovery scoring backfill source protection")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn clear_discovery_scoring_backfill_progress(&self) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring backfill progress clear",
            |conn| {
                conn.execute(
                    "DELETE FROM discovery_scoring_state
                 WHERE state_key IN (
                    'backfill_progress_start_ts',
                    'backfill_progress_cursor_ts',
                    'backfill_progress_cursor_slot',
                    'backfill_progress_cursor_signature'
                 )",
                    [],
                )
                .context("failed clearing discovery scoring backfill progress")?;
                clear_discovery_scoring_seed_boundary_install_marker_on_conn(conn)?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn load_discovery_scoring_covered_since(&self) -> Result<Option<DateTime<Utc>>> {
        self.load_discovery_scoring_state_ts("covered_since_ts")
    }

    pub fn load_discovery_scoring_backfill_progress(
        &self,
    ) -> Result<Option<(DateTime<Utc>, DiscoveryRuntimeCursor)>> {
        let Some(start_ts) = self.load_discovery_scoring_state_ts("backfill_progress_start_ts")?
        else {
            return Ok(None);
        };
        let cursor_ts = self.load_discovery_scoring_state_ts("backfill_progress_cursor_ts")?;
        let slot_raw = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'backfill_progress_cursor_slot'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.backfill_progress_cursor_slot")?;
        let signature = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'backfill_progress_cursor_signature'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context(
                "failed querying discovery_scoring_state.backfill_progress_cursor_signature",
            )?;
        match (cursor_ts, slot_raw, signature) {
            (Some(ts_utc), Some(slot_raw), Some(signature)) => {
                let slot = slot_raw.parse::<u64>().with_context(|| {
                    format!(
                        "invalid discovery_scoring_state.backfill_progress_cursor_slot value: {slot_raw}"
                    )
                })?;
                Ok(Some((
                    start_ts,
                    DiscoveryRuntimeCursor {
                        ts_utc,
                        slot,
                        signature,
                    },
                )))
            }
            _ => Ok(None),
        }
    }

    pub fn load_discovery_scoring_seed_boundary_install_marker(
        &self,
    ) -> Result<Option<DiscoveryScoringSeedBoundaryInstallMarker>> {
        let Some(boundary_start_ts) =
            self.load_discovery_scoring_state_ts("seed_boundary_install_start_ts")?
        else {
            return Ok(None);
        };
        let boundary_cursor_ts =
            self.load_discovery_scoring_state_ts("seed_boundary_install_cursor_ts")?;
        let boundary_cursor_slot_raw = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'seed_boundary_install_cursor_slot'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.seed_boundary_install_cursor_slot")?;
        let boundary_cursor_signature = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'seed_boundary_install_cursor_signature'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context(
                "failed querying discovery_scoring_state.seed_boundary_install_cursor_signature",
            )?;
        match (
            boundary_cursor_ts,
            boundary_cursor_slot_raw,
            boundary_cursor_signature,
        ) {
            (Some(ts_utc), Some(slot_raw), Some(signature)) => {
                let slot = slot_raw.parse::<u64>().with_context(|| {
                    format!(
                        "invalid discovery_scoring_state.seed_boundary_install_cursor_slot value: {slot_raw}"
                    )
                })?;
                Ok(Some(DiscoveryScoringSeedBoundaryInstallMarker {
                    boundary_start_ts,
                    boundary_cursor: DiscoveryRuntimeCursor {
                        ts_utc,
                        slot,
                        signature,
                    },
                }))
            }
            (None, None, None) => Ok(None),
            _ => anyhow::bail!(
                "discovery_scoring_state.seed_boundary_install marker is partially populated"
            ),
        }
    }

    pub fn load_discovery_scoring_materialization_gap_cursor(
        &self,
    ) -> Result<Option<DiscoveryRuntimeCursor>> {
        let Some(ts_utc) = self.load_discovery_scoring_state_ts("materialization_gap_since_ts")?
        else {
            return Ok(None);
        };
        let slot_raw = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'materialization_gap_since_slot'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.materialization_gap_since_slot")?;
        let signature = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'materialization_gap_since_signature'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context(
                "failed querying discovery_scoring_state.materialization_gap_since_signature",
            )?;
        match (slot_raw, signature) {
            (Some(slot_raw), Some(signature)) => {
                let slot = slot_raw.parse::<u64>().with_context(|| {
                    format!(
                        "invalid discovery_scoring_state.materialization_gap_since_slot value: {slot_raw}"
                    )
                })?;
                Ok(Some(DiscoveryRuntimeCursor {
                    ts_utc,
                    slot,
                    signature,
                }))
            }
            _ => Ok(Some(DiscoveryRuntimeCursor {
                ts_utc,
                slot: 0,
                signature: String::new(),
            })),
        }
    }

    pub fn load_discovery_scoring_materialization_gap_repair_target(
        &self,
    ) -> Result<Option<(DiscoveryRuntimeCursor, DiscoveryRuntimeCursor)>> {
        let gap_cursor = self.load_discovery_scoring_cursor_state_exact(
            "materialization_gap_repair_gap_ts",
            "materialization_gap_repair_gap_slot",
            "materialization_gap_repair_gap_signature",
            "materialization_gap_repair_gap",
        )?;
        let target_cursor = self.load_discovery_scoring_cursor_state_exact(
            "materialization_gap_repair_target_ts",
            "materialization_gap_repair_target_slot",
            "materialization_gap_repair_target_signature",
            "materialization_gap_repair_target",
        )?;
        match (gap_cursor, target_cursor) {
            (None, None) => Ok(None),
            (Some(gap_cursor), Some(target_cursor)) => Ok(Some((gap_cursor, target_cursor))),
            _ => Err(anyhow!(
                "discovery_scoring_state.materialization_gap_repair target is partially populated"
            )),
        }
    }

    pub fn load_discovery_scoring_covered_through(&self) -> Result<Option<DateTime<Utc>>> {
        self.load_discovery_scoring_state_ts("covered_through_ts")
    }

    pub fn load_discovery_scoring_covered_through_cursor(
        &self,
    ) -> Result<Option<DiscoveryRuntimeCursor>> {
        let Some(ts_utc) = self.load_discovery_scoring_covered_through()? else {
            return Ok(None);
        };
        let slot_raw = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'covered_through_slot'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.covered_through_slot")?;
        let signature = self
            .conn
            .query_row(
                "SELECT state_value
                 FROM discovery_scoring_state
                 WHERE state_key = 'covered_through_signature'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed querying discovery_scoring_state.covered_through_signature")?;
        let Some(slot_raw) = slot_raw else {
            return Ok(None);
        };
        let Some(signature) = signature else {
            return Ok(None);
        };
        let slot = slot_raw.parse::<u64>().with_context(|| {
            format!("invalid discovery_scoring_state.covered_through_slot value: {slot_raw}")
        })?;
        Ok(Some(DiscoveryRuntimeCursor {
            ts_utc,
            slot,
            signature,
        }))
    }

    pub fn load_discovery_scoring_backfill_protected_since(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>> {
        let Some(expires_at) =
            self.load_discovery_scoring_state_ts("backfill_protect_expires_at")?
        else {
            return Ok(None);
        };
        if expires_at < now {
            return Ok(None);
        }
        self.load_discovery_scoring_state_ts("backfill_protect_since_ts")
    }

    pub fn discovery_scoring_ready_for_window(
        &self,
        window_start: DateTime<Utc>,
        now: DateTime<Utc>,
        max_lag: Duration,
    ) -> Result<bool> {
        let Some(covered_since) = self.load_discovery_scoring_covered_since()? else {
            return Ok(false);
        };
        if self
            .load_discovery_scoring_materialization_gap_cursor()?
            .is_some()
        {
            return Ok(false);
        }
        let Some(covered_through) = self.load_discovery_scoring_covered_through_cursor()? else {
            return Ok(false);
        };
        Ok(covered_since <= window_start && covered_through.ts_utc + max_lag >= now)
    }

    pub fn prune_discovery_scoring_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        self.prune_discovery_scoring_before_batched(cutoff, usize::MAX)
            .map(|summary| summary.deleted_rows)
    }

    pub fn prune_discovery_scoring_before_batched(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<SqliteBatchedDeleteSummary> {
        let mut summary = SqliteBatchedDeleteSummary::default();
        loop {
            let deleted = self.prune_discovery_scoring_before_batch(cutoff, batch_size)?;
            if deleted == 0 {
                break;
            }
            summary.deleted_rows += deleted;
            summary.batches += 1;
        }
        Ok(summary)
    }

    pub fn prune_discovery_scoring_before_batch(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<usize> {
        let cutoff_day = cutoff.date_naive().format("%Y-%m-%d").to_string();
        let cutoff_ts = cutoff.to_rfc3339();
        let cutoff_minute = cutoff.timestamp().div_euclid(60);
        let batch_limit = batch_size.max(1).min(i64::MAX as usize) as i64;
        self.with_immediate_transaction_retry("discovery scoring retention prune batch", |conn| {
            let mut deleted = 0usize;
            let mut remaining = batch_limit;
            if remaining <= 0 {
                return Ok(0usize);
            }

            let buy_deleted = conn
                .execute(
                    "DELETE FROM wallet_scoring_buy_facts
                         WHERE rowid IN (
                             SELECT rowid
                             FROM wallet_scoring_buy_facts
                             WHERE ts < ?1
                             ORDER BY ts ASC, buy_signature ASC
                             LIMIT ?2
                         )",
                    params![&cutoff_ts, remaining],
                )
                .context("failed pruning wallet_scoring_buy_facts")?;
            deleted += buy_deleted;
            remaining -= buy_deleted as i64;

            if remaining > 0 {
                let close_deleted = conn
                    .execute(
                        "DELETE FROM wallet_scoring_close_facts
                             WHERE rowid IN (
                                 SELECT rowid
                                 FROM wallet_scoring_close_facts
                                 WHERE closed_ts < ?1
                                 ORDER BY closed_ts ASC, sell_signature ASC, segment_index ASC
                                 LIMIT ?2
                             )",
                        params![&cutoff_ts, remaining],
                    )
                    .context("failed pruning wallet_scoring_close_facts")?;
                deleted += close_deleted;
                remaining -= close_deleted as i64;
            }

            if remaining > 0 {
                let tx_minutes_deleted = conn
                    .execute(
                        "DELETE FROM wallet_scoring_tx_minutes
                             WHERE rowid IN (
                                 SELECT rowid
                                 FROM wallet_scoring_tx_minutes
                                 WHERE minute_bucket < ?1
                                 ORDER BY minute_bucket ASC, wallet_id ASC
                                 LIMIT ?2
                             )",
                        params![cutoff_minute, remaining],
                    )
                    .context("failed pruning wallet_scoring_tx_minutes")?;
                deleted += tx_minutes_deleted;
                remaining -= tx_minutes_deleted as i64;
            }

            if remaining > 0 {
                let days_deleted = conn
                    .execute(
                        "DELETE FROM wallet_scoring_days
                             WHERE rowid IN (
                                 SELECT rowid
                                 FROM wallet_scoring_days
                                 WHERE activity_day < ?1
                                 ORDER BY activity_day ASC, wallet_id ASC
                                 LIMIT ?2
                             )",
                        params![&cutoff_day, remaining],
                    )
                    .context("failed pruning wallet_scoring_days")?;
                deleted += days_deleted;
            }
            Ok(deleted)
        })
    }

    pub fn has_wallet_scoring_data_since(&self, window_start: DateTime<Utc>) -> Result<bool> {
        let exists = self
            .conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1
                    FROM wallet_scoring_days
                    WHERE activity_day >= ?1
                )",
                params![window_start.date_naive().format("%Y-%m-%d").to_string()],
                |row| row.get::<_, i64>(0),
            )
            .context("failed querying wallet_scoring_days existence")?;
        Ok(exists != 0)
    }

    pub fn load_wallet_scoring_days_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<Vec<WalletScoringDayRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, activity_day, first_seen, last_seen, trades, spent_sol, max_buy_notional_sol
                 FROM wallet_scoring_days
                 WHERE activity_day >= ?1
                 ORDER BY activity_day ASC, wallet_id ASC",
            )
            .context("failed to prepare wallet_scoring_days query")?;
        let mut rows = stmt
            .query(params![window_start
                .date_naive()
                .format("%Y-%m-%d")
                .to_string()])
            .context("failed querying wallet_scoring_days")?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_scoring_days rows")?
        {
            let activity_day_raw: String = row
                .get(1)
                .context("failed reading wallet_scoring_days.activity_day")?;
            let first_seen_raw: String = row
                .get(2)
                .context("failed reading wallet_scoring_days.first_seen")?;
            let last_seen_raw: String = row
                .get(3)
                .context("failed reading wallet_scoring_days.last_seen")?;
            let trades_raw: i64 = row
                .get(4)
                .context("failed reading wallet_scoring_days.trades")?;
            out.push(WalletScoringDayRow {
                wallet_id: row
                    .get(0)
                    .context("failed reading wallet_scoring_days.wallet_id")?,
                activity_day: parse_day(&activity_day_raw, "wallet_scoring_days.activity_day")?,
                first_seen: parse_ts(&first_seen_raw, "wallet_scoring_days.first_seen")?,
                last_seen: parse_ts(&last_seen_raw, "wallet_scoring_days.last_seen")?,
                trades: trades_raw.max(0) as u32,
                spent_sol: row
                    .get(5)
                    .context("failed reading wallet_scoring_days.spent_sol")?,
                max_buy_notional_sol: row
                    .get(6)
                    .context("failed reading wallet_scoring_days.max_buy_notional_sol")?,
            });
        }
        Ok(out)
    }

    pub fn load_wallet_scoring_buy_facts_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<Vec<WalletScoringBuyFactRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, token, ts, notional_sol,
                        market_volume_5m_sol, market_unique_traders_5m, market_liquidity_proxy_sol,
                        quality_source, quality_token_age_seconds, quality_holders, quality_liquidity_sol,
                        rug_check_after_ts, rug_volume_lookahead_sol, rug_unique_traders_lookahead
                 FROM wallet_scoring_buy_facts
                 WHERE ts >= ?1
                 ORDER BY ts ASC, wallet_id ASC",
            )
            .context("failed to prepare wallet_scoring_buy_facts query")?;
        let mut rows = stmt
            .query(params![window_start.to_rfc3339()])
            .context("failed querying wallet_scoring_buy_facts")?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_scoring_buy_facts rows")?
        {
            let ts_raw: String = row
                .get(2)
                .context("failed reading wallet_scoring_buy_facts.ts")?;
            let source_raw: String = row
                .get(7)
                .context("failed reading wallet_scoring_buy_facts.quality_source")?;
            let rug_check_after_raw: String = row
                .get(11)
                .context("failed reading wallet_scoring_buy_facts.rug_check_after_ts")?;
            let market_unique_traders_raw: i64 = row
                .get(5)
                .context("failed reading wallet_scoring_buy_facts.market_unique_traders_5m")?;
            let rug_unique_traders_raw: Option<i64> = row
                .get(13)
                .context("failed reading wallet_scoring_buy_facts.rug_unique_traders_lookahead")?;
            out.push(WalletScoringBuyFactRow {
                wallet_id: row
                    .get(0)
                    .context("failed reading wallet_scoring_buy_facts.wallet_id")?,
                token: row
                    .get(1)
                    .context("failed reading wallet_scoring_buy_facts.token")?,
                ts: parse_ts(&ts_raw, "wallet_scoring_buy_facts.ts")?,
                notional_sol: row
                    .get(3)
                    .context("failed reading wallet_scoring_buy_facts.notional_sol")?,
                market_volume_5m_sol: row
                    .get(4)
                    .context("failed reading wallet_scoring_buy_facts.market_volume_5m_sol")?,
                market_unique_traders_5m: market_unique_traders_raw.max(0) as u32,
                market_liquidity_proxy_sol: row.get(6).context(
                    "failed reading wallet_scoring_buy_facts.market_liquidity_proxy_sol",
                )?,
                quality_source: match source_raw.as_str() {
                    "fresh" => WalletScoringQualitySource::Fresh,
                    "stale" => WalletScoringQualitySource::Stale,
                    "deferred" => WalletScoringQualitySource::Deferred,
                    "missing" => WalletScoringQualitySource::Missing,
                    other => {
                        return Err(anyhow!(
                            "invalid wallet_scoring_buy_facts.quality_source value: {other}"
                        ));
                    }
                },
                quality_token_age_seconds: row
                    .get::<_, Option<i64>>(8)
                    .context("failed reading wallet_scoring_buy_facts.quality_token_age_seconds")?
                    .map(|value| value.max(0) as u64),
                quality_holders: row
                    .get::<_, Option<i64>>(9)
                    .context("failed reading wallet_scoring_buy_facts.quality_holders")?
                    .map(|value| value.max(0) as u64),
                quality_liquidity_sol: row
                    .get(10)
                    .context("failed reading wallet_scoring_buy_facts.quality_liquidity_sol")?,
                rug_check_after_ts: parse_ts(
                    &rug_check_after_raw,
                    "wallet_scoring_buy_facts.rug_check_after_ts",
                )?,
                rug_volume_lookahead_sol: row
                    .get(12)
                    .context("failed reading wallet_scoring_buy_facts.rug_volume_lookahead_sol")?,
                rug_unique_traders_lookahead: rug_unique_traders_raw
                    .map(|value| value.max(0) as u32),
            });
        }
        Ok(out)
    }

    pub fn load_wallet_scoring_close_facts_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<Vec<WalletScoringCloseFactRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, token, closed_ts, pnl_sol, hold_seconds, win
                 FROM wallet_scoring_close_facts
                 WHERE closed_ts >= ?1
                 ORDER BY closed_ts ASC, wallet_id ASC",
            )
            .context("failed to prepare wallet_scoring_close_facts query")?;
        let mut rows = stmt
            .query(params![window_start.to_rfc3339()])
            .context("failed querying wallet_scoring_close_facts")?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_scoring_close_facts rows")?
        {
            let closed_ts_raw: String = row
                .get(2)
                .context("failed reading wallet_scoring_close_facts.closed_ts")?;
            let hold_seconds_raw: i64 = row
                .get(4)
                .context("failed reading wallet_scoring_close_facts.hold_seconds")?;
            let win_raw: i64 = row
                .get(5)
                .context("failed reading wallet_scoring_close_facts.win")?;
            out.push(WalletScoringCloseFactRow {
                wallet_id: row
                    .get(0)
                    .context("failed reading wallet_scoring_close_facts.wallet_id")?,
                token: row
                    .get(1)
                    .context("failed reading wallet_scoring_close_facts.token")?,
                closed_ts: parse_ts(&closed_ts_raw, "wallet_scoring_close_facts.closed_ts")?,
                pnl_sol: row
                    .get(3)
                    .context("failed reading wallet_scoring_close_facts.pnl_sol")?,
                hold_seconds: hold_seconds_raw.max(0),
                win: win_raw != 0,
            });
        }
        Ok(out)
    }

    pub fn wallet_scoring_max_tx_counts_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<HashMap<String, u32>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, MAX(tx_count)
                 FROM wallet_scoring_tx_minutes
                 WHERE minute_bucket >= ?1
                 GROUP BY wallet_id",
            )
            .context("failed to prepare wallet_scoring_tx_minutes max query")?;
        let mut rows = stmt
            .query(params![window_start.timestamp().div_euclid(60)])
            .context("failed querying wallet_scoring_tx_minutes maxima")?;
        let mut out = HashMap::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_scoring_tx_minutes maxima")?
        {
            let wallet_id: String = row
                .get(0)
                .context("failed reading wallet_scoring_tx_minutes.wallet_id")?;
            let max_count_raw: i64 = row
                .get(1)
                .context("failed reading wallet_scoring_tx_minutes max(tx_count)")?;
            out.insert(wallet_id, max_count_raw.max(0) as u32);
        }
        Ok(out)
    }

    pub fn load_wallet_scoring_snapshot_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<WalletScoringSnapshot> {
        self.conn
            .execute_batch("BEGIN DEFERRED TRANSACTION")
            .context("failed to open deferred wallet_scoring snapshot transaction")?;
        let snapshot_result = (|| -> Result<WalletScoringSnapshot> {
            Ok(WalletScoringSnapshot {
                days: self.load_wallet_scoring_days_since(window_start)?,
                buy_facts: self.load_wallet_scoring_buy_facts_since(window_start)?,
                close_facts: self.load_wallet_scoring_close_facts_since(window_start)?,
                max_tx_counts: self.wallet_scoring_max_tx_counts_since(window_start)?,
            })
        })();

        match snapshot_result {
            Ok(snapshot) => {
                self.conn
                    .execute_batch("COMMIT")
                    .context("failed to commit deferred wallet_scoring snapshot transaction")?;
                Ok(snapshot)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_TOKEN: &str = "TokenRugLookahead111111111111111111111111";

    fn test_ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid test timestamp")
            .with_timezone(&Utc)
    }

    fn setup_rug_lookahead_conn() -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE TABLE observed_swaps (
                wallet_id TEXT NOT NULL,
                token_in TEXT NOT NULL,
                token_out TEXT NOT NULL,
                qty_in REAL NOT NULL,
                qty_out REAL NOT NULL,
                ts TEXT NOT NULL
             );
             CREATE INDEX idx_observed_swaps_token_in_out_ts
                ON observed_swaps(token_in, token_out, ts);
             CREATE INDEX idx_observed_swaps_token_out_in_ts
                ON observed_swaps(token_out, token_in, ts);",
        )?;
        Ok(conn)
    }

    fn insert_observed_swap(
        conn: &Connection,
        wallet: &str,
        token_in: &str,
        token_out: &str,
        qty_in: f64,
        qty_out: f64,
        ts: &str,
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO observed_swaps(wallet_id, token_in, token_out, qty_in, qty_out, ts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![wallet, token_in, token_out, qty_in, qty_out, ts],
        )?;
        Ok(())
    }

    #[test]
    fn rug_lookahead_stats_preserve_sol_leg_volume_and_unique_wallet_semantics() -> Result<()> {
        let conn = setup_rug_lookahead_conn()?;
        insert_observed_swap(
            &conn,
            "wallet-a",
            TEST_TOKEN,
            SOL_MINT,
            10.0,
            2.0,
            "2026-04-29T00:00:00+00:00",
        )?;
        insert_observed_swap(
            &conn,
            "wallet-b",
            SOL_MINT,
            TEST_TOKEN,
            3.0,
            30.0,
            "2026-04-29T00:01:00+00:00",
        )?;
        insert_observed_swap(
            &conn,
            "wallet-a",
            SOL_MINT,
            TEST_TOKEN,
            5.0,
            50.0,
            "2026-04-29T00:02:00+00:00",
        )?;
        insert_observed_swap(
            &conn,
            "wallet-irrelevant-token",
            "OtherToken111111111111111111111111111111",
            SOL_MINT,
            1.0,
            99.0,
            "2026-04-29T00:02:00+00:00",
        )?;
        insert_observed_swap(
            &conn,
            "wallet-before",
            TEST_TOKEN,
            SOL_MINT,
            1.0,
            99.0,
            "2026-04-28T23:59:59+00:00",
        )?;
        insert_observed_swap(
            &conn,
            "wallet-after",
            SOL_MINT,
            TEST_TOKEN,
            99.0,
            1.0,
            "2026-04-29T00:05:01+00:00",
        )?;

        let (volume_sol, unique_wallets) = rug_lookahead_stats_on_conn(
            &conn,
            TEST_TOKEN,
            test_ts("2026-04-29T00:00:00Z"),
            test_ts("2026-04-29T00:05:00Z"),
        )?;

        assert!((volume_sol - 10.0).abs() < 1e-9);
        assert_eq!(unique_wallets, 2);
        Ok(())
    }

    #[test]
    fn rug_lookahead_query_plan_uses_existing_token_pair_indexes() -> Result<()> {
        let conn = setup_rug_lookahead_conn()?;
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {RUG_LOOKAHEAD_STATS_QUERY}"))?;
        let mut rows = stmt.query(params![
            TEST_TOKEN,
            SOL_MINT,
            "2026-04-29T00:00:00+00:00",
            "2026-04-29T00:05:00+00:00",
        ])?;
        let mut details = Vec::new();
        while let Some(row) = rows.next()? {
            details.push(row.get::<_, String>(3)?);
        }
        let plan = details.join("\n");
        assert!(
            plan.contains("idx_observed_swaps_token_in_out_ts"),
            "{plan}"
        );
        assert!(
            plan.contains("idx_observed_swaps_token_out_in_ts"),
            "{plan}"
        );
        Ok(())
    }
}
