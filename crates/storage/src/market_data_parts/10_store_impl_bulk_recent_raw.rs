use super::*;

impl SqliteStore {
    pub fn insert_recent_raw_journal_batch_bulk_with_deadline(
        &self,
        swaps: &[SwapEvent],
        completed_at: DateTime<Utc>,
        deadline: Instant,
    ) -> Result<(RecentRawJournalWriteSummary, bool)> {
        self.insert_recent_raw_journal_batch_bulk_with_deadline_internal(
            swaps,
            completed_at,
            deadline,
            None,
        )
    }

    pub(crate) fn insert_recent_raw_journal_batch_bulk_with_deadline_internal(
        &self,
        swaps: &[SwapEvent],
        completed_at: DateTime<Utc>,
        deadline: Instant,
        requested_chunk_rows: Option<usize>,
    ) -> Result<(RecentRawJournalWriteSummary, bool)> {
        self.ensure_recent_raw_journal_tables()?;
        if swaps.is_empty() {
            let state = self.recent_raw_journal_state_cached()?;
            return Ok((recent_raw_journal_write_summary(&state, 0, 0), false));
        }

        let transaction_started = Instant::now();
        let write_result = self.with_immediate_transaction_retry(
            "recent raw journal bulk batch write",
            |conn| {
                ensure_recent_raw_journal_tables_on_conn(conn)?;
                let sqlite_variable_limit = recent_raw_journal_sqlite_variable_limit(conn);
                let chunk_rows = recent_raw_journal_effective_bulk_insert_chunk_rows(
                    requested_chunk_rows,
                    sqlite_variable_limit,
                );
                let mut statement_count = 0usize;
                let mut inserted_rows = 0usize;
                let mut processed_rows = 0usize;
                let mut value_build_duration_ms = 0u64;
                let mut prepare_duration_ms = 0u64;
                let mut execute_duration_ms = 0u64;
                let mut state_refresh_duration_ms = 0u64;
                let mut state_upsert_duration_ms = 0u64;
                let mut deadline_exhausted_before_statement = false;
                let mut deadline_exhausted_during_execute = false;
                let mut time_budget_exhausted = false;

                for chunk in swaps.chunks(chunk_rows) {
                    if Instant::now() >= deadline {
                        deadline_exhausted_before_statement = true;
                        time_budget_exhausted = true;
                        break;
                    }

                    let bind_count = chunk.len() * RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW;
                    if bind_count > sqlite_variable_limit {
                        bail!(
                            "recent raw journal bulk insert chunk would exceed SQLite variable limit: bind_count={bind_count} sqlite_variable_limit={sqlite_variable_limit}"
                        );
                    }

                    let sql = recent_raw_journal_bulk_insert_sql(chunk.len());
                    let value_build_started = Instant::now();
                    let mut values = Vec::with_capacity(
                        chunk
                            .len()
                            .saturating_mul(RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW),
                    );
                    for swap in chunk {
                        push_recent_raw_journal_bulk_insert_values(&mut values, swap);
                    }
                    value_build_duration_ms = value_build_duration_ms
                        .saturating_add(recent_raw_elapsed_ms(value_build_started));
                    let prepare_started = Instant::now();
                    let mut stmt = match conn.prepare_cached(&sql) {
                        Ok(stmt) => stmt,
                        Err(error)
                            if recent_raw_journal_sqlite_error_is_operation_interrupted(&error) =>
                        {
                            prepare_duration_ms = prepare_duration_ms
                                .saturating_add(recent_raw_elapsed_ms(prepare_started));
                            deadline_exhausted_before_statement = true;
                            time_budget_exhausted = true;
                            break;
                        }
                        Err(error) => {
                            return Err(error).context(
                                "failed to prepare recent raw journal bulk insert statement",
                            );
                        }
                    };
                    prepare_duration_ms =
                        prepare_duration_ms.saturating_add(recent_raw_elapsed_ms(prepare_started));
                    if Instant::now() >= deadline {
                        deadline_exhausted_before_statement = true;
                        time_budget_exhausted = true;
                        break;
                    }

                    statement_count = statement_count.saturating_add(1);
                    let execute_started = Instant::now();
                    let execute_result = {
                        let _progress_guard = ProgressHandlerGuard::install(conn, deadline);
                        stmt.execute(params_from_iter(values))
                    };
                    execute_duration_ms =
                        execute_duration_ms.saturating_add(recent_raw_elapsed_ms(execute_started));
                    let changed = match execute_result {
                        Ok(changed) => changed,
                        Err(error)
                            if recent_raw_journal_sqlite_error_is_operation_interrupted(&error) =>
                        {
                            deadline_exhausted_during_execute = true;
                            time_budget_exhausted = true;
                            break;
                        }
                        Err(error) => {
                            return Err(error).context(
                                "failed to bulk insert observed swaps into recent raw journal batch",
                            );
                        }
                    };
                    processed_rows = processed_rows.saturating_add(chunk.len());
                    inserted_rows = inserted_rows.saturating_add(changed);
                    if recent_raw_bulk_write_test_hook_requests_budget_exhaustion(
                        processed_rows,
                        inserted_rows,
                    ) {
                        time_budget_exhausted = true;
                        break;
                    }
                }

                let state_refresh_started = Instant::now();
                let mut state = recent_raw_journal_state_cached_query(conn)?;
                state_refresh_duration_ms = state_refresh_duration_ms
                    .saturating_add(recent_raw_elapsed_ms(state_refresh_started));
                advance_recent_raw_journal_state_for_batch(
                    &mut state,
                    &swaps[..processed_rows],
                    inserted_rows,
                    completed_at,
                );
                if processed_rows > 0 {
                    let state_upsert_started = Instant::now();
                    upsert_recent_raw_journal_state_on_conn(conn, &state)?;
                    state_upsert_duration_ms = state_upsert_duration_ms
                        .saturating_add(recent_raw_elapsed_ms(state_upsert_started));
                }
                let mut summary =
                    recent_raw_journal_write_summary(&state, processed_rows, inserted_rows);
                summary.recent_raw_bulk_sqlite_variable_limit = sqlite_variable_limit;
                summary.recent_raw_bulk_statement_params_per_row =
                    RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW;
                summary.recent_raw_bulk_statement_chunk_row_cap =
                    RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS;
                summary.recent_raw_bulk_effective_statement_chunk_rows = chunk_rows;
                summary.recent_raw_bulk_statement_count = statement_count;
                summary.recent_raw_bulk_rows_processed = processed_rows;
                summary.recent_raw_bulk_rows_inserted = inserted_rows;
                summary.recent_raw_bulk_value_build_duration_ms = value_build_duration_ms;
                summary.recent_raw_bulk_prepare_duration_ms = prepare_duration_ms;
                summary.recent_raw_bulk_execute_duration_ms = execute_duration_ms;
                summary.recent_raw_bulk_state_refresh_duration_ms = state_refresh_duration_ms;
                summary.recent_raw_bulk_state_upsert_duration_ms = state_upsert_duration_ms;
                summary.recent_raw_bulk_deadline_exhausted_before_statement =
                    deadline_exhausted_before_statement;
                summary.recent_raw_bulk_deadline_exhausted_during_execute =
                    deadline_exhausted_during_execute;
                Ok((summary, time_budget_exhausted))
            },
        );
        match write_result {
            Ok((mut summary, time_budget_exhausted)) => {
                summary.recent_raw_bulk_transaction_duration_ms =
                    recent_raw_elapsed_ms(transaction_started);
                Ok((summary, time_budget_exhausted))
            }
            Err(error) if recent_raw_journal_anyhow_error_is_operation_interrupted(&error) => {
                let state = self.recent_raw_journal_state_cached()?;
                Ok((recent_raw_journal_write_summary(&state, 0, 0), true))
            }
            Err(error) => Err(error),
        }
    }
}
