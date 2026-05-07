use super::*;

impl SqliteStore {
    pub fn insert_recent_raw_journal_batch(
        &self,
        swaps: &[SwapEvent],
        completed_at: DateTime<Utc>,
    ) -> Result<RecentRawJournalWriteSummary> {
        let (summary, _time_budget_exhausted) =
            self.insert_recent_raw_journal_batch_internal(swaps, completed_at, None)?;
        Ok(summary)
    }

    pub fn insert_recent_raw_journal_batch_with_deadline(
        &self,
        swaps: &[SwapEvent],
        completed_at: DateTime<Utc>,
        deadline: Instant,
    ) -> Result<(RecentRawJournalWriteSummary, bool)> {
        self.insert_recent_raw_journal_batch_internal(swaps, completed_at, Some(deadline))
    }

    fn insert_recent_raw_journal_batch_internal(
        &self,
        swaps: &[SwapEvent],
        completed_at: DateTime<Utc>,
        deadline: Option<Instant>,
    ) -> Result<(RecentRawJournalWriteSummary, bool)> {
        self.ensure_recent_raw_journal_tables()?;
        if swaps.is_empty() {
            let state = self.recent_raw_journal_state_cached()?;
            return Ok((recent_raw_journal_write_summary(&state, 0, 0), false));
        }

        self.with_immediate_transaction_retry("recent raw journal batch write", |conn| {
            ensure_recent_raw_journal_tables_on_conn(conn)?;
            let mut inserted_rows = 0usize;
            let mut processed_rows = 0usize;
            let mut time_budget_exhausted = false;
            {
                let _progress_guard =
                    deadline.map(|deadline| ProgressHandlerGuard::install(conn, deadline));
                let mut stmt = conn
                    .prepare_cached(
                        "INSERT OR IGNORE INTO observed_swaps(
                            signature,
                            wallet_id,
                            dex,
                            token_in,
                            token_out,
                            qty_in,
                            qty_out,
                            qty_in_raw,
                            qty_in_decimals,
                            qty_out_raw,
                            qty_out_decimals,
                            slot,
                            ts
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    )
                    .context("failed to prepare recent raw journal batch insert statement")?;

                for swap in swaps {
                    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        time_budget_exhausted = true;
                        break;
                    }
                    let changed = match stmt.execute(params![
                        &swap.signature,
                        &swap.wallet,
                        &swap.dex,
                        &swap.token_in,
                        &swap.token_out,
                        swap.amount_in,
                        swap.amount_out,
                        swap.exact_amounts
                            .as_ref()
                            .map(|value| value.amount_in_raw.as_str()),
                        swap.exact_amounts
                            .as_ref()
                            .map(|value| i64::from(value.amount_in_decimals)),
                        swap.exact_amounts
                            .as_ref()
                            .map(|value| value.amount_out_raw.as_str()),
                        swap.exact_amounts
                            .as_ref()
                            .map(|value| i64::from(value.amount_out_decimals)),
                        swap.slot as i64,
                        swap.ts_utc.to_rfc3339(),
                    ]) {
                        Ok(changed) => changed,
                        Err(error) => {
                            if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                                time_budget_exhausted = true;
                                break;
                            }
                            return Err(error).context(
                                "failed to insert observed swap into recent raw journal batch",
                            );
                        }
                    };
                    processed_rows = processed_rows.saturating_add(1);
                    if changed > 0 {
                        inserted_rows = inserted_rows.saturating_add(1);
                    }
                }
            }

            let mut state = recent_raw_journal_state_cached_query(conn)?;
            advance_recent_raw_journal_state_for_batch(
                &mut state,
                &swaps[..processed_rows],
                inserted_rows,
                completed_at,
            );
            if processed_rows > 0 {
                upsert_recent_raw_journal_state_on_conn(conn, &state)?;
            }
            Ok((
                recent_raw_journal_write_summary(&state, processed_rows, inserted_rows),
                time_budget_exhausted,
            ))
        })
    }
}
