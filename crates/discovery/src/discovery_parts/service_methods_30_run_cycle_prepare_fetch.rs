use super::*;

pub(crate) struct RunCycleWindowFetchPreparation {
    pub(crate) publish_due: bool,
    pub(crate) fetch_progress: FetchProgress,
    pub(crate) delta_fetched: usize,
    pub(crate) swaps_evicted_due_cap: usize,
    pub(crate) swaps_warm_loaded: usize,
}

impl DiscoveryService {
    pub(crate) fn prepare_run_cycle_window_fetch(
        &self,
        store: &SqliteStore,
        state: &mut DiscoveryWindowState,
        now: DateTime<Utc>,
        publish_interval_seconds: i64,
        window_start: DateTime<Utc>,
        max_window_swaps_in_memory: usize,
        fetch_limit: usize,
        fetch_page_limit: usize,
        fetch_time_budget: StdDuration,
        short_retention_window: bool,
    ) -> Result<RunCycleWindowFetchPreparation> {
        let mut delta_fetched = 0usize;
        let mut swaps_evicted_due_cap = 0usize;
        let mut swaps_warm_loaded = 0usize;
        state.evict_before(window_start);
        state.clear_cap_truncation_if_window_caught_up(window_start);
        if !short_retention_window {
            state.bootstrap_from_persisted_metrics = false;
        }
        let publish_due = state.last_publish_at.map_or(true, |last_publish_at| {
            now.signed_duration_since(last_publish_at).num_seconds() >= publish_interval_seconds
        });
        let mut out_of_order = false;
        let mut cursor_restored_from_store = false;
        if state.cursor.is_none() {
            let restored = match store.load_discovery_runtime_cursor() {
                Ok(cursor) => cursor,
                Err(error) => {
                    if discovery_runtime_cursor_load_error_requires_abort(&error) {
                        return Err(error)
                            .context("failed loading discovery runtime cursor with fatal sqlite I/O");
                    }
                    warn!(
                        error = %error,
                        "failed loading discovery runtime cursor; falling back to window_start bootstrap"
                    );
                    None
                }
            };
            cursor_restored_from_store = restored.is_some();
            let restored = restored.map(|cursor| DiscoveryCursor {
                ts_utc: cursor.ts_utc,
                slot: cursor.slot,
                signature: cursor.signature,
            });
            state.cursor = Some(restored.unwrap_or_else(|| DiscoveryCursor::bootstrap(window_start)));
        }

        let mut cursor = state
            .cursor
            .clone()
            .unwrap_or_else(|| DiscoveryCursor::bootstrap(window_start));
        if cursor.ts_utc < window_start {
            cursor = DiscoveryCursor::bootstrap(window_start);
        }
        if state.swaps.is_empty() && cursor_restored_from_store {
            if short_retention_window {
                state.bootstrap_from_persisted_metrics = true;
            }
            match store.load_recent_observed_swaps_since(window_start, max_window_swaps_in_memory) {
                Ok((swaps, truncated_by_limit)) => {
                    for swap in swaps {
                        if let Some(back) = state.swaps.back() {
                            if cmp_swap_order(&swap, back) == Ordering::Less {
                                out_of_order = true;
                            }
                        }
                        if state.signatures.insert(swap.signature.clone()) {
                            swaps_evicted_due_cap = swaps_evicted_due_cap
                                .saturating_add(
                                    state.push_swap_capped(swap, max_window_swaps_in_memory),
                                );
                            swaps_warm_loaded = swaps_warm_loaded.saturating_add(1);
                        }
                    }
                    if truncated_by_limit {
                        state.mark_warm_load_truncated();
                        maybe_arm_cap_truncation_deactivation_guard(
                            state,
                            now,
                            CapTruncationDeactivationGuardReason::WarmLoadTruncated,
                        );
                    }
                }
                Err(error) => {
                    if discovery_recent_window_load_error_requires_abort(&error) {
                        return Err(error).context(
                            "failed warm-loading discovery window from sqlite recent slice with fatal sqlite I/O",
                        );
                    }
                    warn!(
                        error = %error,
                        "failed warm-loading discovery window from sqlite recent slice"
                    );
                }
            }
        }
        if !state.swaps.is_empty() {
            state.bootstrap_from_persisted_metrics = false;
        }

        let mut fetch_progress = FetchProgress::default();
        let fetch_deadline = Instant::now() + fetch_time_budget;
        loop {
            if fetch_progress.pages >= fetch_page_limit {
                fetch_progress.page_budget_exhausted =
                    fetch_progress.query_rows_last_page >= fetch_limit;
                break;
            }
            if Instant::now() >= fetch_deadline {
                fetch_progress.time_budget_exhausted = true;
                break;
            }

            let cursor_signature = cursor.signature.clone();
            let page_result = store.for_each_observed_swap_after_cursor_with_budget(
                cursor.ts_utc,
                cursor.slot,
                cursor_signature.as_str(),
                fetch_limit,
                fetch_deadline,
                |swap| {
                    cursor = DiscoveryCursor::from_swap(&swap);
                    if swap.ts_utc < window_start {
                        return Ok(());
                    }
                    if state.signatures.contains(&swap.signature) {
                        return Ok(());
                    }
                    if let Some(back) = state.swaps.back() {
                        if cmp_swap_order(&swap, back) == Ordering::Less {
                            out_of_order = true;
                        }
                    }
                    state.signatures.insert(swap.signature.clone());
                    let evicted = state.push_swap_capped(swap, max_window_swaps_in_memory);
                    if evicted > 0 {
                        maybe_arm_cap_truncation_deactivation_guard(
                            state,
                            now,
                            CapTruncationDeactivationGuardReason::LiveCapEviction,
                        );
                    }
                    swaps_evicted_due_cap = swaps_evicted_due_cap.saturating_add(evicted);
                    delta_fetched = delta_fetched.saturating_add(1);
                    Ok(())
                },
            )?;
            let page_rows = page_result.rows_seen;
            fetch_progress.pages = fetch_progress.pages.saturating_add(1);
            fetch_progress.query_rows = fetch_progress.query_rows.saturating_add(page_rows);
            fetch_progress.query_rows_last_page = page_rows;
            fetch_progress.time_budget_exhausted |= page_result.time_budget_exhausted;

            if page_result.time_budget_exhausted || page_rows < fetch_limit {
                break;
            }
        }
        fetch_progress.saturated =
            fetch_progress.page_budget_exhausted || fetch_progress.time_budget_exhausted;

        if fetch_progress.query_rows > 0 {
            state.cursor = Some(cursor.clone());
            let persisted = DiscoveryRuntimeCursor {
                ts_utc: cursor.ts_utc,
                slot: cursor.slot,
                signature: cursor.signature,
            };
            if let Err(error) = store.upsert_discovery_runtime_cursor(&persisted) {
                if discovery_runtime_cursor_error_requires_abort(&error) {
                    return Err(error)
                        .context("failed persisting discovery runtime cursor with fatal sqlite I/O");
                }
                warn!(
                    error = %error,
                    "failed persisting discovery runtime cursor"
                );
            }
        }

        if out_of_order {
            let mut sorted: Vec<SwapEvent> = state.swaps.drain(..).collect();
            sorted.sort_by(cmp_swap_order);
            state.swaps = sorted.into();
        }

        Ok(RunCycleWindowFetchPreparation {
            publish_due,
            fetch_progress,
            delta_fetched,
            swaps_evicted_due_cap,
            swaps_warm_loaded,
        })
    }
}
