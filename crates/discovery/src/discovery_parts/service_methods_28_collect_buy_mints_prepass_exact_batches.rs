use crate::*;

pub(crate) struct CollectBuyMintsExactBatchAdvance {
    pub(crate) rows_processed: usize,
    pub(crate) pages_processed: usize,
    pub(crate) unique_buy_mints_discovered: usize,
    pub(crate) time_budget_exhausted: bool,
}

impl DiscoveryService {
    pub(crate) fn advance_reconcile_expired_head_exact_batch(
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        source_window_start: DateTime<Utc>,
        active_pending_mints: &[String],
        deadline: Instant,
    ) -> Result<CollectBuyMintsExactBatchAdvance> {
        let last_active_pending_mint = active_pending_mints
            .last()
            .cloned()
            .expect("expired-head exact batch requires pending mints");
        if active_pending_mints.len() == 1 {
            let exact_mint = last_active_pending_mint;
            let exact_count = store.count_observed_buy_mint_occurrences_in_time_bounds_with_budget(
                source_window_start,
                true,
                state.window_start,
                false,
                &exact_mint,
                deadline,
            )?;
            state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor = None;
            if exact_count.time_budget_exhausted {
                return Ok(CollectBuyMintsExactBatchAdvance {
                    rows_processed: 0,
                    pages_processed: 1,
                    unique_buy_mints_discovered: 0,
                    time_budget_exhausted: true,
                });
            }
            Self::subtract_buy_mint_occurrences(
                &mut state.payload,
                &exact_mint,
                exact_count.buy_count,
            );
            state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor_token = Some(exact_mint);
            Self::clear_reconcile_expired_head_pending_batch(&mut state.payload);
            return Ok(CollectBuyMintsExactBatchAdvance {
                rows_processed: 1,
                pages_processed: 1,
                unique_buy_mints_discovered: 0,
                time_budget_exhausted: false,
            });
        }

        #[allow(unused_mut)]
        let mut page = store
            .load_observed_buy_mint_counts_for_exact_tokens_in_time_bounds_with_budget(
                source_window_start,
                true,
                state.window_start,
                false,
                active_pending_mints,
                deadline,
            )?;
        #[cfg(test)]
        if let Some(limit) = take_test_force_reconcile_expired_head_exact_batch_row_limit() {
            if page.rows.len() > limit {
                page.rows.truncate(limit);
                page.time_budget_exhausted = true;
            }
        }
        for row in &page.rows {
            Self::subtract_buy_mint_occurrences(&mut state.payload, &row.mint, row.buy_count);
        }
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor = None;
        let last_accounted_cursor = if page.time_budget_exhausted {
            page.rows.last().map(|row| row.mint.clone())
        } else {
            Some(last_active_pending_mint)
        };
        if let Some(last_accounted_cursor) = last_accounted_cursor {
            let processed_prefix_len = if page.time_budget_exhausted {
                active_pending_mints
                    .partition_point(|mint| mint.as_str() <= last_accounted_cursor.as_str())
            } else {
                active_pending_mints.len()
            };
            if processed_prefix_len > 0 {
                state
                    .payload
                    .collect_buy_mints_reconcile_expired_head_pending_mints
                    .drain(..processed_prefix_len);
            }
            state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor_token =
                Some(last_accounted_cursor);
        }
        Ok(CollectBuyMintsExactBatchAdvance {
            rows_processed: page.rows.len(),
            pages_processed: 1,
            unique_buy_mints_discovered: 0,
            time_budget_exhausted: page.time_budget_exhausted,
        })
    }

    pub(crate) fn advance_reconcile_new_tail_exact_batch(
        store: &SqliteStore,
        state: &mut PersistedStreamRebuildState,
        source_horizon_end: DateTime<Utc>,
        active_pending_mints: &[String],
        reconcile_cursor_token: Option<&str>,
        deadline: Instant,
    ) -> Result<CollectBuyMintsExactBatchAdvance> {
        #[allow(unused_mut)]
        let mut page = store
            .load_observed_buy_mint_counts_for_exact_tokens_in_time_bounds_with_budget(
                source_horizon_end,
                false,
                state.horizon_end,
                true,
                active_pending_mints,
                deadline,
            )?;
        #[cfg(test)]
        if let Some(limit) = take_test_force_reconcile_new_tail_exact_batch_row_limit() {
            if page.rows.len() > limit {
                page.rows.truncate(limit);
                page.time_budget_exhausted = true;
            }
        }
        #[cfg(test)]
        if take_test_force_reconcile_new_tail_zero_row_timeout() {
            page.rows.clear();
            page.time_budget_exhausted = true;
        }
        if page.rows.is_empty() {
            let candidate_mints = active_pending_mints.len();
            let narrowed_slice_end_token = Self::narrowed_stale_reconcile_slice_end(
                active_pending_mints,
            )
            .unwrap_or_else(|| {
                active_pending_mints
                    .last()
                    .cloned()
                    .expect("pending stale new-tail batch end token")
            });
            state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
            Self::clear_reconcile_new_tail_pending_batch(&mut state.payload);
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token =
                Some(narrowed_slice_end_token.clone());
            info!(
                rebuild_window_start = %state.window_start,
                rebuild_horizon_end = %state.horizon_end,
                rebuild_collect_buy_mints_reconcile_new_tail_cursor_token =
                    reconcile_cursor_token,
                rebuild_collect_buy_mints_reconcile_new_tail_slice_end_token =
                    Some(narrowed_slice_end_token.as_str()),
                rebuild_collect_buy_mints_reconcile_new_tail_candidate_mints =
                    candidate_mints,
                "stale new-tail exact candidate batch returned no rows before completion; narrowing exact token slice before retry"
            );
            return Ok(CollectBuyMintsExactBatchAdvance {
                rows_processed: 0,
                pages_processed: 1,
                unique_buy_mints_discovered: 0,
                time_budget_exhausted: true,
            });
        }

        let mut unique_buy_mints_discovered = 0usize;
        for row in &page.rows {
            if Self::add_buy_mint_occurrences(&mut state.payload, &row.mint, row.buy_count) {
                unique_buy_mints_discovered = unique_buy_mints_discovered.saturating_add(1);
            }
        }
        state.payload.collect_buy_mints_reconcile_new_tail_cursor = None;
        let last_processed_cursor = page.rows.last().map(|row| row.mint.clone());
        if let Some(last_processed_cursor) = last_processed_cursor.clone() {
            let processed_prefix_len = active_pending_mints
                .partition_point(|mint| mint.as_str() <= last_processed_cursor.as_str());
            if processed_prefix_len > 0 {
                state
                    .payload
                    .collect_buy_mints_reconcile_new_tail_pending_mints
                    .drain(..processed_prefix_len);
            }
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token =
                Some(last_processed_cursor.clone());
            if state
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token
                .as_deref()
                == Some(last_processed_cursor.as_str())
            {
                state
                    .payload
                    .collect_buy_mints_reconcile_new_tail_slice_end_token = None;
            }
        }
        Ok(CollectBuyMintsExactBatchAdvance {
            rows_processed: page.rows.len(),
            pages_processed: 1,
            unique_buy_mints_discovered,
            time_budget_exhausted: page.time_budget_exhausted,
        })
    }
}
