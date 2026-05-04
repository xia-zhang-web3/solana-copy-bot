impl SqliteStore {
    pub fn for_each_observed_sol_leg_swap_in_window_after_cursor_for_target_buy_mints_with_budget<
        F,
    >(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
        target_buy_mints: &[String],
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSolLegCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        if target_buy_mints.is_empty() {
            return self.for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget(
                since, until, cursor, limit, deadline, on_swap,
            );
        }

        self.ensure_loaded_observed_sol_leg_target_buy_mint_filter(target_buy_mints)?;
        let access_path = self.observed_sol_leg_cursor_access_path()?;
        if limit == 0 {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: false,
                access_path,
            });
        }
        if Instant::now() >= deadline {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: true,
                access_path,
            });
        }

        let mut total_rows_seen = 0usize;
        let mut time_budget_exhausted = false;
        let mut page_cursor = cursor.cloned();

        while total_rows_seen < limit && Instant::now() < deadline {
            let page_limit = limit
                .saturating_sub(total_rows_seen)
                .min(OBSERVED_SOL_LEG_CURSOR_QUERY_PAGE_LIMIT);
            let mut next_cursor = page_cursor.clone();
            let page = self
                .for_each_observed_sol_leg_swap_in_window_after_cursor_single_statement_for_loaded_target_buy_mint_filter_with_budget(
                    since,
                    until,
                    page_cursor.as_ref(),
                    page_limit,
                    deadline,
                    |swap| {
                        next_cursor = Some(DiscoveryRuntimeCursor {
                            ts_utc: swap.ts_utc,
                            slot: swap.slot,
                            signature: swap.signature.clone(),
                        });
                        on_swap(swap)
                    },
                )?;
            total_rows_seen = total_rows_seen.saturating_add(page.rows_seen);
            time_budget_exhausted |= page.time_budget_exhausted;
            if page.time_budget_exhausted || page.rows_seen < page_limit {
                return Ok(ObservedSolLegCursorPage {
                    rows_seen: total_rows_seen,
                    time_budget_exhausted,
                    access_path,
                });
            }
            page_cursor = next_cursor;
        }

        if Instant::now() >= deadline && total_rows_seen < limit {
            time_budget_exhausted = true;
        }

        Ok(ObservedSolLegCursorPage {
            rows_seen: total_rows_seen,
            time_budget_exhausted,
            access_path,
        })
    }

    fn for_each_observed_sol_leg_swap_in_window_after_cursor_single_statement_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSolLegCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        let access_path = self.observed_sol_leg_cursor_access_path()?;
        if limit == 0 {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: false,
                access_path,
            });
        }
        if Instant::now() >= deadline {
            return Ok(ObservedSolLegCursorPage {
                rows_seen: 0,
                time_budget_exhausted: true,
                access_path,
            });
        }

        let index_hint = match access_path {
            ObservedSolLegCursorAccessPath::SolLegPartialIndex => {
                "INDEXED BY idx_observed_swaps_sol_leg_ts_slot_signature"
            }
            ObservedSolLegCursorAccessPath::TsCursorFallback => {
                "INDEXED BY idx_observed_swaps_ts_slot_signature"
            }
        };
        let limit = (limit.min(i64::MAX as usize)) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let (query, params): (String, Vec<rusqlite::types::Value>) = match cursor {
            Some(cursor) => (
                format!(
                    "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                            qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                     FROM observed_swaps {index_hint}
                     WHERE ts >= ?1
                       AND ts <= ?2
                       AND (token_in = '{SOL_MINT}' OR token_out = '{SOL_MINT}')
                       AND (ts, slot, signature) > (?3, ?4, ?5)
                     ORDER BY ts ASC, slot ASC, signature ASC
                     LIMIT ?6"
                ),
                vec![
                    since.to_rfc3339().into(),
                    until.to_rfc3339().into(),
                    cursor.ts_utc.to_rfc3339().into(),
                    (cursor.slot as i64).into(),
                    cursor.signature.clone().into(),
                    limit.into(),
                ],
            ),
            None => (
                format!(
                    "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                            qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                     FROM observed_swaps {index_hint}
                     WHERE ts >= ?1
                       AND ts <= ?2
                       AND (token_in = '{SOL_MINT}' OR token_out = '{SOL_MINT}')
                     ORDER BY ts ASC, slot ASC, signature ASC
                     LIMIT ?3"
                ),
                vec![
                    since.to_rfc3339().into(),
                    until.to_rfc3339().into(),
                    limit.into(),
                ],
            ),
        };
        let mut stmt = self
            .conn
            .prepare(&query)
            .context("failed to prepare observed_swaps sol-leg window cursor query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed to query observed_swaps sol-leg window cursor")?;

        let mut seen = 0usize;
        let mut time_budget_exhausted = false;
        loop {
            let next_row = match rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        time_budget_exhausted = true;
                        break;
                    }
                    return Err(error)
                        .context("failed iterating observed_swaps sol-leg window cursor rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            let swap = Self::row_to_swap_event(row)?;
            on_swap(swap)?;
            seen = seen.saturating_add(1);
        }
        Ok(ObservedSolLegCursorPage {
            rows_seen: seen,
            time_budget_exhausted,
            access_path,
        })
    }

}
