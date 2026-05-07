use super::*;

impl SqliteStore {
    pub fn apply_discovery_scoring_repair_micro_commit_lock_first(
        &self,
        config: &DiscoveryAggregateWriteConfig,
        expected_gap_cursor: &DiscoveryRuntimeCursor,
        expected_repair_target: &DiscoveryRuntimeCursor,
        max_rows: usize,
        max_lock_duration: StdDuration,
        defer_rug_lookahead_on_budget_hotspot: bool,
    ) -> Result<(
        &'static str,
        Option<DiscoveryRuntimeCursor>,
        Option<DiscoveryRuntimeCursor>,
        usize,
        bool,
        bool,
        bool,
        bool,
        usize,
        usize,
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
                    return Ok(repair_micro_commit_missing_current_outcome(
                        "covered_missing",
                    ));
                };

                let Some(gap_cursor) =
                    load_discovery_scoring_materialization_gap_cursor_on_conn(conn)?
                else {
                    return Ok(repair_micro_commit_current_zero_outcome(
                        "gap_missing",
                        current,
                        false,
                        false,
                    ));
                };
                if cmp_cursor_order(&gap_cursor, expected_gap_cursor) != Ordering::Equal {
                    return Ok(repair_micro_commit_current_zero_outcome(
                        "state_mismatch",
                        current,
                        false,
                        false,
                    ));
                }

                let Some((target_gap_cursor, repair_target)) =
                    load_discovery_scoring_materialization_gap_repair_target_on_conn(conn)?
                else {
                    return Ok(repair_micro_commit_current_zero_outcome(
                        "target_missing",
                        current,
                        false,
                        false,
                    ));
                };
                if cmp_cursor_order(&target_gap_cursor, expected_gap_cursor) != Ordering::Equal
                    || cmp_cursor_order(&repair_target, expected_repair_target) != Ordering::Equal
                {
                    return Ok(repair_micro_commit_current_zero_outcome(
                        "state_mismatch",
                        current,
                        false,
                        false,
                    ));
                }

                let exact_gap_exists =
                    observed_swap_exact_cursor_exists_on_conn(conn, &gap_cursor)?;
                if !exact_gap_exists {
                    return Ok(repair_micro_commit_current_zero_outcome(
                        "exact_gap_missing",
                        current,
                        false,
                        false,
                    ));
                }

                let gap_observed_before = cmp_cursor_order(&current, &gap_cursor) != Ordering::Less;
                if cmp_cursor_order(&current, &repair_target) != Ordering::Less {
                    return Ok(repair_micro_commit_current_zero_outcome(
                        "already_at_target",
                        current,
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
                        return Ok(repair_micro_commit_current_zero_outcome(
                            "budget_exhausted_without_progress",
                            current,
                            false,
                            gap_observed_before,
                        ));
                    }
                    return Ok(repair_micro_commit_current_zero_outcome(
                        "no_rows",
                        current,
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
                    return Ok(repair_micro_commit_current_zero_outcome(
                        "budget_exhausted_without_progress",
                        current,
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
                let rug_finalize_outcome = if defer_rug_lookahead_on_budget_hotspot {
                    finalize_repair_prefix_rug_facts_defer_budget_hotspot_on_conn(conn, &swaps)?
                } else {
                    finalize_mature_rug_facts_on_conn(conn, last_cursor.ts_utc)?;
                    RugLookaheadFinalizeOutcome::default()
                };
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
                Ok(repair_micro_commit_outcome(
                    "committed",
                    Some(current),
                    Some(last_cursor),
                    swaps.len(),
                    reached_target,
                    gap_observed,
                    rug_finalize_outcome.deferred_due_to_budget_hotspot,
                    rug_finalize_outcome.batch_prefetch_used,
                    rug_finalize_outcome.exact_count,
                    rug_finalize_outcome.deferred_count,
                ))
            },
        )
    }
}
