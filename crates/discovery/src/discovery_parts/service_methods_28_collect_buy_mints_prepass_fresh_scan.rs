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
