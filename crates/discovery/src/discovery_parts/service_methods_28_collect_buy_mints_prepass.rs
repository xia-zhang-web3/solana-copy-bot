use crate::*;

impl DiscoveryService {
    pub(crate) fn advance_persisted_stream_prepass(
        &self,
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        collect_buy_mints_phase_page_limit_override: Option<usize>,
        deadline: Instant,
    ) -> Result<PersistedStreamPhaseAdvance> {
        let mut rows_processed = 0usize;
        let mut pages_processed = 0usize;
        let mut unique_buy_mints_discovered = 0usize;
        let mut cursor_token = state.payload.collect_buy_mints_cursor_token.clone();
        let phase_page_limit = collect_buy_mints_phase_page_limit_override
            .unwrap_or(fetch_page_limit)
            .max(1);
        if state.payload.collect_buy_mints_mode == CollectBuyMintsMode::FreshScan
            && cursor_token.is_none()
            && state.phase_cursor.is_some()
        {
            {
let mut canonical_mints = state.payload.unique_buy_mints.clone();
Self::canonicalize_unique_buy_mints(&mut canonical_mints);
info!(
    rebuild_window_start = %state.window_start,
    rebuild_horizon_end = %state.horizon_end,
    rebuild_legacy_phase_cursor_ts =
        ?state.phase_cursor.as_ref().map(|cursor| cursor.ts_utc),
    rebuild_legacy_phase_cursor_slot =
        state.phase_cursor.as_ref().map(|cursor| cursor.slot),
    rebuild_legacy_phase_cursor_signature = state
        .phase_cursor
        .as_ref()
        .map(|cursor| cursor.signature.as_str()),
    rebuild_legacy_unique_buy_mints = state.payload.unique_buy_mints.len(),
    rebuild_legacy_canonical_unique_buy_mints = canonical_mints.len(),
    "migrating collect_buy_mints checkpoint from raw-swap cursor replay to direct distinct SOL-buy mint pagination with canonical safe-prefix recovery"
);
let (safe_prefix_len, time_budget_exhausted) = self
    .derive_legacy_collect_buy_mints_safe_prefix_len(store, state, &canonical_mints, deadline)?;
if time_budget_exhausted {
    return Ok(PersistedStreamPhaseAdvance {
        rows_processed: 0,
        pages_processed: 0,
        replay_wallet_stats_rows_processed: 0,
        replay_wallet_stats_pages_processed: 0,
        replay_sol_leg_rows_processed: 0,
        replay_sol_leg_pages_processed: 0,
        replay_sol_leg_elapsed_ms: 0,
        replay_wallet_stats_day_count_source_progress:
            ReplayWalletStatsDayCountSourceProgress::default(),
        replay_sol_leg_access_path: None,
        source_exhausted: false,
        phase_cursor: state.phase_cursor.clone(),
        collect_buy_mints_cursor_token: None,
        unique_buy_mints_discovered: 0,
        budget_exhausted_reason: Some(PersistedStreamBudgetExhaustedReason::TimeBudget),
    });
}
let dropped_legacy_tail = canonical_mints.len().saturating_sub(safe_prefix_len);
state.payload.unique_buy_mints = canonical_mints.into_iter().take(safe_prefix_len).collect();
cursor_token = state.payload.unique_buy_mints.last().cloned();
info!(
    rebuild_window_start = %state.window_start,
    rebuild_horizon_end = %state.horizon_end,
    rebuild_collect_buy_mints_cursor_token = cursor_token.as_deref(),
    rebuild_legacy_safe_prefix_mints = safe_prefix_len,
    rebuild_legacy_tail_mints_dropped = dropped_legacy_tail,
    "recovered canonical safe-prefix progress for legacy collect_buy_mints checkpoint before resuming direct distinct mint pagination"
);
}
;
        }
        let budget_exhausted_reason = loop {
            if pages_processed >= phase_page_limit {
                break Some(PersistedStreamBudgetExhaustedReason::PageBudget);
            }
            if Instant::now() >= deadline {
                break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
            }

            match state.payload.collect_buy_mints_mode {
                CollectBuyMintsMode::FreshScan => {
                    {
let fresh_scan_deadline = Self::collect_buy_mints_fresh_scan_work_deadline(state, deadline);
if Instant::now() >= fresh_scan_deadline {
    break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
}
let fresh_scan_fetch_limit = Self::collect_buy_mints_fresh_scan_batch_size(fetch_limit);
let previous_cursor_token = cursor_token.clone();
#[allow(unused_mut)]
let mut page = store.load_observed_buy_mint_counts_in_window_after_token_with_budget(
    state.window_start,
    state.horizon_end,
    cursor_token.as_deref(),
    None,
    fresh_scan_fetch_limit,
    fresh_scan_deadline,
)?;
#[cfg(test)]
if let Some(limit) = take_test_force_collect_buy_mints_fresh_scan_row_limit() {
    if page.rows.len() > limit {
        page.rows.truncate(limit);
        page.time_budget_exhausted = true;
    }
}
pages_processed = pages_processed.saturating_add(1);
rows_processed = rows_processed.saturating_add(page.rows.len());
for row in &page.rows {
    if Self::set_buy_mint_occurrences(&mut state.payload, &row.mint, row.buy_count) {
        unique_buy_mints_discovered = unique_buy_mints_discovered.saturating_add(1);
    }
}
cursor_token = page.rows.last().map(|row| row.mint.clone());
if page.time_budget_exhausted {
    info!(
        rebuild_window_start = %state.window_start,
        rebuild_horizon_end = %state.horizon_end,
        rebuild_collect_buy_mints_requested_fetch_limit = fetch_limit,
        rebuild_collect_buy_mints_effective_fetch_limit =
            fresh_scan_fetch_limit,
        rebuild_collect_buy_mints_previous_cursor_token =
            previous_cursor_token.as_deref(),
        rebuild_collect_buy_mints_next_cursor_token =
            cursor_token.as_deref(),
        rebuild_collect_buy_mints_cursor_advanced =
            cursor_token != previous_cursor_token,
        rebuild_collect_buy_mints_page_rows = page.rows.len(),
        rebuild_collect_buy_mints_cycle_unique_buy_mints_discovered =
            unique_buy_mints_discovered,
        rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
        rebuild_quality_next_mint_index =
            state.payload.token_quality_progress.next_mint_index,
        rebuild_collect_buy_mints_transition_requires_source_exhaustion =
            true,
        "collect_buy_mints fresh-scan hit its bounded time budget; persisting grouped mint cursor progress before the next recovery cycle"
    );
}
if page.time_budget_exhausted {
    break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
}
if page.rows.len() < fresh_scan_fetch_limit {
    state.payload.collect_buy_mints_prepass_complete = true;
    state.payload.collect_buy_mints_cursor_token = None;
    if Self::payload_has_exact_buy_mint_membership(&state.payload) {
        Self::sync_unique_buy_mints_from_counts(&mut state.payload);
    } else {
        Self::canonicalize_unique_buy_mints(&mut state.payload.unique_buy_mints);
    }
    return Ok(PersistedStreamPhaseAdvance {
        rows_processed,
        pages_processed,
        replay_wallet_stats_rows_processed: 0,
        replay_wallet_stats_pages_processed: 0,
        replay_sol_leg_rows_processed: 0,
        replay_sol_leg_pages_processed: 0,
        replay_sol_leg_elapsed_ms: 0,
        replay_wallet_stats_day_count_source_progress:
            ReplayWalletStatsDayCountSourceProgress::default(),
        replay_sol_leg_access_path: None,
        source_exhausted: true,
        phase_cursor: None,
        collect_buy_mints_cursor_token: None,
        unique_buy_mints_discovered,
        budget_exhausted_reason: None,
    });
}
}
;
                }
                CollectBuyMintsMode::ReconcileExpiredHead => {
                    {
let Some(source_window_start) = state
    .payload
    .collect_buy_mints_reconcile_source_window_start
else {
    state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;
    Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
    continue;
};
if source_window_start >= state.window_start {
    state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;
    state
        .payload
        .collect_buy_mints_reconcile_expired_head_cursor = None;
    state
        .payload
        .collect_buy_mints_reconcile_expired_head_cursor_token = None;
    Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
    continue;
}

let reconcile_cursor_token = state
    .payload
    .collect_buy_mints_reconcile_expired_head_cursor_token
    .clone();
let reconcile_batch_size = Self::stale_reconcile_token_batch_size(fetch_limit);
let exact_count_batch_size = Self::stale_reconcile_exact_count_batch_size(fetch_limit);
if state
    .payload
    .collect_buy_mints_reconcile_expired_head_pending_mints
    .is_empty()
{
    let candidate_page = store.load_observed_buy_mints_in_time_bounds_after_token_with_budget(
        source_window_start,
        true,
        state.window_start,
        false,
        reconcile_cursor_token.as_deref(),
        None,
        reconcile_batch_size,
        deadline,
    )?;
    if candidate_page.mints.is_empty() {
        if candidate_page.time_budget_exhausted {
            break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
        }
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor = None;
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor_token = None;
        Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
        info!(
            rebuild_window_start = %state.window_start,
            rebuild_horizon_end = %state.horizon_end,
            rebuild_reconcile_source_window_start = ?state
                .payload
                .collect_buy_mints_reconcile_source_window_start,
            rebuild_reconcile_source_horizon_end = ?state
                .payload
                .collect_buy_mints_reconcile_source_horizon_end,
            rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
            "completed expired-head reconciliation for carried-forward collect_buy_mints state; switching to new-tail reconciliation"
        );
        continue;
    }
    state
        .payload
        .collect_buy_mints_reconcile_expired_head_pending_mints = candidate_page.mints;
    if candidate_page.time_budget_exhausted {
        break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
    }
}

let active_pending_mints = state
    .payload
    .collect_buy_mints_reconcile_expired_head_pending_mints
    .iter()
    .take(exact_count_batch_size)
    .cloned()
    .collect::<Vec<_>>();
if active_pending_mints.is_empty() {
    state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;
    state
        .payload
        .collect_buy_mints_reconcile_expired_head_cursor = None;
    state
        .payload
        .collect_buy_mints_reconcile_expired_head_cursor_token = None;
    Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
    info!(
        rebuild_window_start = %state.window_start,
        rebuild_horizon_end = %state.horizon_end,
        rebuild_reconcile_source_window_start = ?state
            .payload
            .collect_buy_mints_reconcile_source_window_start,
        rebuild_reconcile_source_horizon_end = ?state
            .payload
            .collect_buy_mints_reconcile_source_horizon_end,
        rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
        "completed expired-head reconciliation for carried-forward collect_buy_mints state; switching to new-tail reconciliation"
    );
    continue;
}

let exact_batch = Self::advance_reconcile_expired_head_exact_batch(
    store,
    state,
    source_window_start,
    &active_pending_mints,
    deadline,
)?;
pages_processed = pages_processed.saturating_add(exact_batch.pages_processed);
rows_processed = rows_processed.saturating_add(exact_batch.rows_processed);
if exact_batch.time_budget_exhausted {
    break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
}

if !state
    .payload
    .collect_buy_mints_reconcile_expired_head_pending_mints
    .is_empty()
{
    continue;
}
continue;
}
;
                }
                CollectBuyMintsMode::ReconcileNewTail => {
                    {
let Some(source_horizon_end) = state
    .payload
    .collect_buy_mints_reconcile_source_horizon_end
else {
    state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
    continue;
};
if source_horizon_end >= state.horizon_end {
    state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
    state
        .payload
        .collect_buy_mints_reconcile_new_tail_cursor_token = None;
    state
        .payload
        .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
    Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
    Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
    state
        .payload
        .collect_buy_mints_reconcile_source_window_start = None;
    state.payload.collect_buy_mints_reconcile_source_horizon_end = None;
    if state.payload.collect_buy_mints_prepass_complete {
        return Ok(PersistedStreamPhaseAdvance {
            rows_processed,
            pages_processed,
            replay_wallet_stats_rows_processed: 0,
            replay_wallet_stats_pages_processed: 0,
            replay_sol_leg_rows_processed: 0,
            replay_sol_leg_pages_processed: 0,
            replay_sol_leg_elapsed_ms: 0,
            replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            replay_sol_leg_access_path: None,
            source_exhausted: true,
            phase_cursor: None,
            collect_buy_mints_cursor_token: None,
            unique_buy_mints_discovered,
            budget_exhausted_reason: None,
        });
    }
    state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
    continue;
}

let reconcile_cursor_token = state
    .payload
    .collect_buy_mints_reconcile_new_tail_cursor_token
    .clone();
let reconcile_batch_size = Self::stale_reconcile_token_batch_size(fetch_limit);
let exact_count_batch_size = Self::stale_reconcile_exact_count_batch_size(fetch_limit);
if state
    .payload
    .collect_buy_mints_reconcile_new_tail_pending_mints
    .is_empty()
{
    let pending_slice_end_token = state
        .payload
        .collect_buy_mints_reconcile_new_tail_slice_end_token
        .clone();
    let candidate_page = store.load_observed_buy_mints_in_time_bounds_after_token_with_budget(
        source_horizon_end,
        false,
        state.horizon_end,
        true,
        reconcile_cursor_token.as_deref(),
        pending_slice_end_token.as_deref(),
        reconcile_batch_size,
        deadline,
    )?;
    let Some(_) = candidate_page.mints.last() else {
        if candidate_page.time_budget_exhausted {
            break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
        }
        if pending_slice_end_token.is_some() {
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
            continue;
        }
        state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token = None;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
        Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
        state
            .payload
            .collect_buy_mints_reconcile_source_window_start = None;
        state.payload.collect_buy_mints_reconcile_source_horizon_end = None;
        if state.payload.collect_buy_mints_prepass_complete {
            return Ok(PersistedStreamPhaseAdvance {
                rows_processed,
                pages_processed,
                replay_wallet_stats_rows_processed: 0,
                replay_wallet_stats_pages_processed: 0,
                replay_sol_leg_rows_processed: 0,
                replay_sol_leg_pages_processed: 0,
                replay_sol_leg_elapsed_ms: 0,
                replay_wallet_stats_day_count_source_progress:
                    ReplayWalletStatsDayCountSourceProgress::default(),
                replay_sol_leg_access_path: None,
                source_exhausted: true,
                phase_cursor: None,
                collect_buy_mints_cursor_token: None,
                unique_buy_mints_discovered,
                budget_exhausted_reason: None,
            });
        }
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::FreshScan;
        info!(
            rebuild_window_start = %state.window_start,
            rebuild_horizon_end = %state.horizon_end,
            rebuild_collect_buy_mints_cursor_token =
                state.payload.collect_buy_mints_cursor_token.as_deref(),
            rebuild_unique_buy_mints = state.payload.unique_buy_mints.len(),
            "completed new-tail reconciliation for carried-forward collect_buy_mints state; resuming canonical distinct mint scan from persisted cursor"
        );
        continue;
    };
    state
        .payload
        .collect_buy_mints_reconcile_new_tail_pending_mints = candidate_page.mints;
    if candidate_page.time_budget_exhausted {
        break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
    }
}

let active_pending_mints = state
    .payload
    .collect_buy_mints_reconcile_new_tail_pending_mints
    .iter()
    .take(exact_count_batch_size)
    .cloned()
    .collect::<Vec<_>>();

if active_pending_mints.len() == 1 {
    let exact_mint = state
        .payload
        .collect_buy_mints_reconcile_new_tail_pending_mints
        .first()
        .cloned()
        .expect("single pending mint");
    let exact_count = store.count_observed_buy_mint_occurrences_in_time_bounds_with_budget(
        source_horizon_end,
        false,
        state.horizon_end,
        true,
        &exact_mint,
        deadline,
    )?;
    pages_processed = pages_processed.saturating_add(1);
    state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
    if exact_count.time_budget_exhausted {
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_slice_end_token = Some(exact_mint);
        break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
    }
    rows_processed = rows_processed.saturating_add(1);
    if Self::add_buy_mint_occurrences(&mut state.payload, &exact_mint, exact_count.buy_count) {
        unique_buy_mints_discovered = unique_buy_mints_discovered.saturating_add(1);
    }
    state
        .payload
        .collect_buy_mints_reconcile_new_tail_cursor_token = Some(exact_mint);
    if state
        .payload
        .collect_buy_mints_reconcile_new_tail_slice_end_token
        .as_deref()
        == state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token
            .as_deref()
    {
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
    }
    Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
    if state
        .payload
        .collect_buy_mints_reconcile_new_tail_slice_end_token
        .is_some()
    {
        continue;
    }
    continue;
}

let exact_batch = Self::advance_reconcile_new_tail_exact_batch(
    store,
    state,
    source_horizon_end,
    &active_pending_mints,
    reconcile_cursor_token.as_deref(),
    deadline,
)?;
pages_processed = pages_processed.saturating_add(exact_batch.pages_processed);
rows_processed = rows_processed.saturating_add(exact_batch.rows_processed);
unique_buy_mints_discovered =
    unique_buy_mints_discovered.saturating_add(exact_batch.unique_buy_mints_discovered);
if exact_batch.time_budget_exhausted {
    break Some(PersistedStreamBudgetExhaustedReason::TimeBudget);
}
if !state
    .payload
    .collect_buy_mints_reconcile_new_tail_pending_mints
    .is_empty()
{
    continue;
}
}
;
                }
            }
        };

        let _ = self.maybe_warm_collect_buy_mints_token_quality_prefix(
            store,
            state,
            fetch_limit,
            deadline,
        )?;

        Ok(PersistedStreamPhaseAdvance {
            rows_processed,
            pages_processed,
            replay_wallet_stats_rows_processed: 0,
            replay_wallet_stats_pages_processed: 0,
            replay_sol_leg_rows_processed: 0,
            replay_sol_leg_pages_processed: 0,
            replay_sol_leg_elapsed_ms: 0,
            replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            replay_sol_leg_access_path: None,
            source_exhausted: false,
            phase_cursor: None,
            collect_buy_mints_cursor_token: cursor_token,
            unique_buy_mints_discovered,
            budget_exhausted_reason,
        })
    }
}
