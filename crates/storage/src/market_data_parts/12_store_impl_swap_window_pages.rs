impl SqliteStore {
    pub fn for_each_observed_swap_in_window_paged<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: usize,
        mut on_swap: F,
    ) -> Result<usize>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        Ok(self
            .for_each_observed_swap_in_window_paged_with_budget(
                since,
                until,
                limit,
                Instant::now() + StdDuration::from_secs(24 * 60 * 60),
                |swap| on_swap(swap),
            )?
            .rows_seen)
    }

    pub fn for_each_observed_swap_in_window_paged_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
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
        let mut cursor: Option<DiscoveryRuntimeCursor> = None;

        loop {
            let page_cursor = cursor.clone();
            let mut next_cursor = page_cursor.clone();
            let page = self.for_each_observed_swap_in_window_after_cursor_with_budget(
                since,
                until,
                page_cursor.as_ref(),
                limit,
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
            cursor = next_cursor;
            total_rows_seen = total_rows_seen.saturating_add(page.rows_seen);
            time_budget_exhausted |= page.time_budget_exhausted;
            if page.time_budget_exhausted || page.rows_seen < limit {
                break;
            }
        }

        Ok(ObservedSwapCursorPage {
            rows_seen: total_rows_seen,
            time_budget_exhausted,
        })
    }
}
