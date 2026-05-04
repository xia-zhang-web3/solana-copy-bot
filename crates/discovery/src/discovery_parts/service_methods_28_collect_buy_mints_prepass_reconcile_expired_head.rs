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
