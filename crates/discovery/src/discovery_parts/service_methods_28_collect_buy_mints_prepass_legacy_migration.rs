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
