struct AggregateReplayFetchedPage {
    page: Vec<SwapEvent>,
    rows_seen: usize,
    saw_row_after_repair_target: bool,
    next_reached_repair_target: bool,
    next_gap_cursor_observed: bool,
    reached_rows_after_unobserved_gap: bool,
}

fn fetch_aggregate_replay_page(
    store: &SqliteStore,
    config: &ObservedSwapWriterConfig,
    cursor: &DiscoveryRuntimeCursor,
    gap_cursor: Option<&DiscoveryRuntimeCursor>,
    repair_target_cursor: Option<&DiscoveryRuntimeCursor>,
    gap_cursor_observed_before: bool,
    initial_gap_cursor_observed: bool,
    max_pages: Option<usize>,
    progress: &AggregateReplayProgress,
    initial_reached_repair_target: bool,
) -> Result<AggregateReplayFetchedPage> {
    let mut page = Vec::with_capacity(config.batch_max_size);
    let mut saw_row_after_repair_target = false;
    let mut reached_repair_target = initial_reached_repair_target;
    let mut gap_cursor_observed = initial_gap_cursor_observed;
    let page_fetch_started = Instant::now();
    log_discovery_aggregate_phase(
        DISCOVERY_AGGREGATE_PHASE_PAGE_FETCH_START,
        None,
        gap_cursor,
        None,
        repair_target_cursor,
        Some(cursor),
        None,
        gap_cursor_observed_before,
        max_pages,
        None,
        progress.page_count,
        progress.last_replay_cursor.as_ref(),
        reached_repair_target,
        progress.caught_up_to_tail,
        false,
        0,
    );
    let rows_seen = store.for_each_observed_swap_after_cursor(
        cursor.ts_utc,
        cursor.slot,
        cursor.signature.as_str(),
        config.batch_max_size,
        |swap| {
            let swap_cursor = DiscoveryRuntimeCursor {
                ts_utc: swap.ts_utc,
                slot: swap.slot,
                signature: swap.signature.clone(),
            };
            if let Some(repair_target_cursor) = repair_target_cursor {
                match compare_discovery_runtime_cursors(&swap_cursor, repair_target_cursor) {
                    std::cmp::Ordering::Greater => {
                        saw_row_after_repair_target = true;
                        return Ok(());
                    }
                    std::cmp::Ordering::Equal => {
                        reached_repair_target = true;
                    }
                    std::cmp::Ordering::Less => {}
                }
            }
            if gap_cursor.is_some_and(|gap_cursor| {
                gap_cursor.ts_utc == swap.ts_utc
                    && gap_cursor.slot == swap.slot
                    && gap_cursor.signature == swap.signature
            }) {
                gap_cursor_observed = true;
            }
            page.push(swap);
            Ok(())
        },
    )?;
    log_discovery_aggregate_phase(
        DISCOVERY_AGGREGATE_PHASE_PAGE_FETCH_END,
        None,
        gap_cursor,
        None,
        repair_target_cursor,
        Some(cursor),
        None,
        gap_cursor_observed_before,
        max_pages,
        Some(page.len()),
        progress.page_count,
        progress.last_replay_cursor.as_ref(),
        reached_repair_target,
        progress.caught_up_to_tail,
        false,
        elapsed_ms_ceil(page_fetch_started.elapsed()),
    );
    let reached_rows_after_unobserved_gap = gap_cursor.is_some_and(|gap_cursor| {
        !gap_cursor_observed
            && page.iter().any(|swap| {
                compare_discovery_runtime_cursors(
                    &DiscoveryRuntimeCursor {
                        ts_utc: swap.ts_utc,
                        slot: swap.slot,
                        signature: swap.signature.clone(),
                    },
                    gap_cursor,
                ) == std::cmp::Ordering::Greater
            })
    });
    if let Some(gap_cursor) = gap_cursor {
        if reached_rows_after_unobserved_gap {
            warn!(
                materialization_gap_ts = %gap_cursor.ts_utc,
                materialization_gap_slot = gap_cursor.slot,
                materialization_gap_signature = %gap_cursor.signature,
                page_rows = page.len(),
                "discovery scoring aggregate replay reached rows after an unobserved materialization gap; leaving coverage and gap latch unchanged",
            );
        }
    }
    Ok(AggregateReplayFetchedPage {
        page,
        rows_seen,
        saw_row_after_repair_target,
        next_reached_repair_target: reached_repair_target,
        next_gap_cursor_observed: gap_cursor_observed,
        reached_rows_after_unobserved_gap,
    })
}
