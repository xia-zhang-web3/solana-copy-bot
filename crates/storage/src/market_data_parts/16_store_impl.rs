impl SqliteStore {
    pub fn for_each_observed_swap_after_cursor<F>(
        &self,
        cursor_ts: DateTime<Utc>,
        cursor_slot: u64,
        cursor_signature: &str,
        limit: usize,
        mut on_swap: F,
    ) -> Result<usize>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        Ok(self
            .for_each_observed_swap_after_cursor_with_budget(
                cursor_ts,
                cursor_slot,
                cursor_signature,
                limit,
                Instant::now() + StdDuration::from_secs(24 * 60 * 60),
                |swap| on_swap(swap),
            )?
            .rows_seen)
    }

    pub fn observed_swap_exact_cursor_exists(
        &self,
        cursor: &DiscoveryRuntimeCursor,
    ) -> Result<bool> {
        let slot = i64::try_from(cursor.slot).with_context(|| {
            format!(
                "observed_swaps exact cursor slot overflows i64: {}",
                cursor.slot
            )
        })?;
        let found = self
            .conn
            .query_row(
                "SELECT 1
                 FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
                 WHERE ts = ?1 AND slot = ?2 AND signature = ?3
                 LIMIT 1",
                params![cursor.ts_utc.to_rfc3339(), slot, cursor.signature.as_str()],
                |_row| Ok(()),
            )
            .optional()
            .context("failed loading observed_swaps exact cursor row")?
            .is_some();
        Ok(found)
    }

    pub fn for_each_observed_swap_after_cursor_with_budget<F>(
        &self,
        cursor_ts: DateTime<Utc>,
        cursor_slot: u64,
        cursor_signature: &str,
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        if limit == 0 {
            return Ok(ObservedSwapCursorPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedSwapCursorPage {
                rows_seen: 0,
                time_budget_exhausted: true,
            });
        }
        let mut total_rows_seen = 0usize;
        let mut time_budget_exhausted = false;
        let mut page_cursor = DiscoveryRuntimeCursor {
            ts_utc: cursor_ts,
            slot: cursor_slot,
            signature: cursor_signature.to_string(),
        };

        while total_rows_seen < limit && Instant::now() < deadline {
            let page_limit = limit
                .saturating_sub(total_rows_seen)
                .min(OBSERVED_SWAP_CURSOR_QUERY_PAGE_LIMIT);
            let mut next_cursor = page_cursor.clone();
            let page = self.for_each_observed_swap_after_cursor_single_statement_with_budget(
                page_cursor.ts_utc,
                page_cursor.slot,
                page_cursor.signature.as_str(),
                page_limit,
                deadline,
                |swap| {
                    next_cursor = DiscoveryRuntimeCursor {
                        ts_utc: swap.ts_utc,
                        slot: swap.slot,
                        signature: swap.signature.clone(),
                    };
                    on_swap(swap)
                },
            )?;
            total_rows_seen = total_rows_seen.saturating_add(page.rows_seen);
            time_budget_exhausted |= page.time_budget_exhausted;
            if page.time_budget_exhausted || page.rows_seen < page_limit {
                break;
            }
            page_cursor = next_cursor;
        }

        if Instant::now() >= deadline && total_rows_seen < limit {
            time_budget_exhausted = true;
        }

        Ok(ObservedSwapCursorPage {
            rows_seen: total_rows_seen,
            time_budget_exhausted,
        })
    }

    fn for_each_observed_swap_after_cursor_single_statement_with_budget<F>(
        &self,
        cursor_ts: DateTime<Utc>,
        cursor_slot: u64,
        cursor_signature: &str,
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        if limit == 0 {
            return Ok(ObservedSwapCursorPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedSwapCursorPage {
                rows_seen: 0,
                time_budget_exhausted: true,
            });
        }

        let limit = (limit.min(i64::MAX as usize)) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let mut stmt = self
            .conn
            .prepare(OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY)
            .context("failed to prepare observed_swaps cursor query")?;
        let mut rows = stmt
            .query(params![
                cursor_ts.to_rfc3339(),
                cursor_slot as i64,
                cursor_signature,
                limit,
            ])
            .context("failed to query observed_swaps by cursor")?;

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
                    return Err(error).context("failed iterating observed_swaps cursor rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            let swap = Self::row_to_swap_event(row)?;
            on_swap(swap)?;
            seen = seen.saturating_add(1);
        }
        Ok(ObservedSwapCursorPage {
            rows_seen: seen,
            time_budget_exhausted,
        })
    }

    pub fn load_recent_observed_swaps_since(
        &self,
        since: DateTime<Utc>,
        limit: usize,
    ) -> Result<(Vec<SwapEvent>, bool)> {
        if limit == 0 {
            return Ok((Vec::new(), false));
        }
        let retained_limit = limit.min(i64::MAX as usize);
        let query_limit = retained_limit.saturating_add(1).min(i64::MAX as usize) as i64;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 WHERE ts >= ?1
                 ORDER BY ts DESC, slot DESC, signature DESC
                 LIMIT ?2",
            )
            .context("failed to prepare recent observed_swaps query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339(), query_limit])
            .context("failed to query recent observed_swaps")?;

        let mut swaps = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating recent observed_swaps rows")?
        {
            swaps.push(Self::row_to_swap_event(row)?);
        }
        let truncated_by_limit = swaps.len() > retained_limit;
        if truncated_by_limit {
            swaps.truncate(retained_limit);
        }
        swaps.reverse();
        Ok((swaps, truncated_by_limit))
    }
}
