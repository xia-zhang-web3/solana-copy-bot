#[cfg(test)]
fn run_aggregate_gap_replay(
    store: &SqliteStore,
    config: &ObservedSwapWriterConfig,
    max_pages: Option<usize>,
) -> Result<AggregateReplayProgress> {
    run_aggregate_gap_replay_with_resume(store, config, max_pages, None, false, None)
}

fn run_aggregate_gap_replay_with_resume(
    store: &SqliteStore,
    config: &ObservedSwapWriterConfig,
    max_pages: Option<usize>,
    resume_after_cursor: Option<&DiscoveryRuntimeCursor>,
    gap_cursor_observed_before: bool,
    repair_target_cursor: Option<&DiscoveryRuntimeCursor>,
) -> Result<AggregateReplayProgress> {
    if !config.aggregate_writes_enabled {
        return Ok(AggregateReplayProgress::default());
    }

    let covered_since = store.load_discovery_scoring_covered_since()?;
    let mut cursor = match store.load_discovery_scoring_covered_through_cursor()? {
        Some(cursor) => cursor,
        None => {
            if covered_since.is_some() {
                return Err(anyhow!(
                    "aggregate writes require an exact covered_through cursor for safe startup replay"
                ));
            }
            return Ok(AggregateReplayProgress::default());
        }
    };
    let gap_cursor = store.load_discovery_scoring_materialization_gap_cursor()?;
    let mut progress = AggregateReplayProgress {
        gap_cursor_loaded: gap_cursor.is_some(),
        ..AggregateReplayProgress::default()
    };
    if let Some(resume_after_cursor) = resume_after_cursor {
        cursor = resume_after_cursor.clone();
    } else if let Some(gap_cursor) = gap_cursor.as_ref() {
        if compare_discovery_runtime_cursors(gap_cursor, &cursor) != std::cmp::Ordering::Greater {
            cursor = aggregate_replay_cursor_before(gap_cursor);
        }
    }
    let mut reached_repair_target = repair_target_cursor.is_some_and(|target| {
        compare_discovery_runtime_cursors(&cursor, target) != std::cmp::Ordering::Less
    });
    let mut gap_cursor_observed = gap_cursor_observed_before;

    loop {
        if reached_repair_target {
            break;
        }
        if max_pages.is_some_and(|limit| progress.page_count >= limit.max(1)) {
            break;
        }
        let fetched_page = fetch_aggregate_replay_page(
            store,
            config,
            &cursor,
            gap_cursor.as_ref(),
            repair_target_cursor,
            gap_cursor_observed_before,
            gap_cursor_observed,
            max_pages,
            &progress,
            reached_repair_target,
        )?;
        let AggregateReplayFetchedPage {
            page,
            rows_seen,
            saw_row_after_repair_target,
            next_reached_repair_target,
            next_gap_cursor_observed,
            reached_rows_after_unobserved_gap,
        } = fetched_page;
        progress.last_page_rows = page.len();
        reached_repair_target = next_reached_repair_target;
        gap_cursor_observed = next_gap_cursor_observed;
        if page.is_empty() {
            progress.caught_up_to_tail = !saw_row_after_repair_target;
            break;
        }
        if reached_rows_after_unobserved_gap {
            progress.gap_cursor_observed = false;
            break;
        }

        if apply_aggregate_replay_page(
            store,
            config,
            &page,
            &cursor,
            gap_cursor.as_ref(),
            repair_target_cursor,
            gap_cursor_observed_before,
            gap_cursor_observed,
            max_pages,
            reached_repair_target,
            &mut progress,
        )? {
            return Ok(progress);
        }

        let last_swap = page
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("aggregate startup replay page unexpectedly empty"))?;
        if finalize_aggregate_replay_rug_facts(
            store,
            &page,
            &last_swap,
            &cursor,
            gap_cursor.as_ref(),
            repair_target_cursor,
            gap_cursor_observed_before,
            gap_cursor_observed,
            max_pages,
            reached_repair_target,
            &mut progress,
        )? {
            return Ok(progress);
        }
        cursor = DiscoveryRuntimeCursor {
            ts_utc: last_swap.ts_utc,
            slot: last_swap.slot,
            signature: last_swap.signature.clone(),
        };
        if update_aggregate_replay_covered_through(
            store,
            &page,
            &cursor,
            gap_cursor.as_ref(),
            repair_target_cursor,
            gap_cursor_observed_before,
            gap_cursor_observed,
            max_pages,
            reached_repair_target,
            &mut progress,
        )? {
            return Ok(progress);
        }
        progress.last_replay_cursor = Some(cursor.clone());
        progress.page_count = progress.page_count.saturating_add(1);

        if repair_target_cursor.is_some_and(|target| {
            compare_discovery_runtime_cursors(&cursor, target) != std::cmp::Ordering::Less
        }) {
            reached_repair_target = true;
            break;
        }

        if rows_seen < config.batch_max_size {
            progress.caught_up_to_tail = true;
            break;
        }
    }

    progress.reached_repair_target = reached_repair_target;
    let clear_ready = gap_cursor_observed
        && repair_target_cursor.map_or(progress.caught_up_to_tail, |_| reached_repair_target);
    if clear_ready {
        if let Some(gap_cursor) = gap_cursor.as_ref() {
            store.clear_discovery_scoring_materialization_gap_if_cursor_observed(gap_cursor)?;
        }
    }

    progress.gap_cursor_observed = gap_cursor_observed;
    Ok(progress)
}
