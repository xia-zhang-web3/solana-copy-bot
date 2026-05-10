use super::*;

pub(super) fn observed_swap_exact_cursor_exists_on_conn(
    conn: &Connection,
    cursor: &DiscoveryRuntimeCursor,
) -> Result<bool> {
    validate_observed_swaps_timestamps_canonical_utc(conn)?;
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

pub(super) fn load_observed_swaps_after_cursor_for_repair_on_conn(
    conn: &Connection,
    cursor: &DiscoveryRuntimeCursor,
    repair_target: &DiscoveryRuntimeCursor,
    limit: usize,
) -> Result<(Vec<SwapEvent>, bool)> {
    if limit == 0 {
        return Ok((Vec::new(), false));
    }
    validate_observed_swaps_timestamps_canonical_utc(conn)?;
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
pub(super) fn lock_first_repair_budget_after_rows_for_tests() -> Option<usize> {
    DISCOVERY_SCORING_LOCK_FIRST_REPAIR_BUDGET_AFTER_ROWS.with(|failpoint| failpoint.get())
}

#[cfg(not(debug_assertions))]
pub(super) fn lock_first_repair_budget_after_rows_for_tests() -> Option<usize> {
    None
}

pub(super) fn lock_first_repair_budget_reached(collected_rows: usize) -> bool {
    lock_first_repair_budget_after_rows_for_tests().is_some_and(|limit| collected_rows >= limit)
}

#[cfg(debug_assertions)]
pub(super) struct LockFirstRepairCurrentRowsGuard;

#[cfg(debug_assertions)]
impl LockFirstRepairCurrentRowsGuard {
    pub(super) fn install(rows: usize) -> Self {
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
pub(super) struct LockFirstRepairCurrentRowsGuard;

#[cfg(not(debug_assertions))]
impl LockFirstRepairCurrentRowsGuard {
    pub(super) fn install(_rows: usize) -> Self {
        Self
    }
}

#[cfg(debug_assertions)]
pub(super) fn rug_lookahead_budget_failpoint_triggered() -> bool {
    let current_rows =
        DISCOVERY_SCORING_LOCK_FIRST_REPAIR_CURRENT_ROWS.with(|current| current.get());
    DISCOVERY_SCORING_RUG_LOOKAHEAD_BUDGET_FAIL_ABOVE_ROWS
        .with(|limit| limit.get())
        .is_some_and(|limit| current_rows > limit)
}

#[cfg(not(debug_assertions))]
pub(super) fn rug_lookahead_budget_failpoint_triggered() -> bool {
    false
}

#[cfg(debug_assertions)]
pub(super) fn rug_lookahead_batch_budget_failpoint_triggered() -> bool {
    let current_rows =
        DISCOVERY_SCORING_LOCK_FIRST_REPAIR_CURRENT_ROWS.with(|current| current.get());
    DISCOVERY_SCORING_RUG_LOOKAHEAD_BATCH_BUDGET_FAIL_ABOVE_ROWS
        .with(|limit| limit.get())
        .is_some_and(|limit| current_rows > limit)
}

#[cfg(not(debug_assertions))]
pub(super) fn rug_lookahead_batch_budget_failpoint_triggered() -> bool {
    false
}

#[cfg(debug_assertions)]
pub(super) fn rug_lookahead_unknown_failpoint_triggered() -> bool {
    DISCOVERY_SCORING_RUG_LOOKAHEAD_UNKNOWN_FAILPOINT.with(|failpoint| failpoint.get())
}

#[cfg(not(debug_assertions))]
pub(super) fn rug_lookahead_unknown_failpoint_triggered() -> bool {
    false
}

#[cfg(debug_assertions)]
pub(super) fn note_rug_lookahead_stats_call_for_tests() {
    DISCOVERY_SCORING_RUG_LOOKAHEAD_STATS_CALL_COUNT
        .with(|counter| counter.set(counter.get().saturating_add(1)));
}

#[cfg(not(debug_assertions))]
pub(super) fn note_rug_lookahead_stats_call_for_tests() {}

pub(super) fn check_lock_first_repair_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        anyhow::bail!(DISCOVERY_AGGREGATE_REPAIR_LOCK_FIRST_BUDGET_EXHAUSTED_WITHOUT_PROGRESS);
    }
    Ok(())
}

pub(super) fn upsert_discovery_scoring_materialization_gap_repair_target_on_conn(
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
