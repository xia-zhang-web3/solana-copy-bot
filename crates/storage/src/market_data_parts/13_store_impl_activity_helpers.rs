use super::*;

impl SqliteStore {
    pub(super) fn observed_wallet_activity_day_summaries_in_window_with_budget(
        &self,
        wallet_ids: &[String],
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        deadline: Instant,
    ) -> Result<ObservedWalletActivityDaySummaryPage> {
        if wallet_ids.is_empty() {
            return Ok(ObservedWalletActivityDaySummaryPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedWalletActivityDaySummaryPage {
                rows: HashMap::new(),
                time_budget_exhausted: true,
            });
        }

        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let placeholders = std::iter::repeat_n("?", wallet_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let day_start = since.date_naive().format("%Y-%m-%d").to_string();
        let day_end = until.date_naive().format("%Y-%m-%d").to_string();
        let sql = format!(
            "SELECT wallet_id,
                    COUNT(*),
                    MAX(CASE WHEN activity_day = ?1 THEN 1 ELSE 0 END),
                    MAX(CASE WHEN activity_day = ?2 THEN 1 ELSE 0 END)
             FROM wallet_activity_days INDEXED BY idx_wallet_activity_days_day_wallet
             WHERE activity_day >= ?1
               AND activity_day <= ?2
               AND wallet_id IN ({placeholders})
             GROUP BY wallet_id"
        );
        let mut params = vec![
            rusqlite::types::Value::from(day_start),
            rusqlite::types::Value::from(day_end),
        ];
        params.extend(wallet_ids.iter().cloned().map(rusqlite::types::Value::from));
        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("failed to prepare wallet_activity_days summary query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed querying wallet_activity_days summaries")?;
        let mut activity_day_summaries = HashMap::new();
        loop {
            let next_row = match rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        return Ok(ObservedWalletActivityDaySummaryPage {
                            rows: HashMap::new(),
                            time_budget_exhausted: true,
                        });
                    }
                    return Err(error).context("failed iterating wallet_activity_days summaries");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            let wallet_id: String = row
                .get(0)
                .context("failed reading wallet_activity_days summary wallet_id")?;
            let inclusive_day_count: i64 = row
                .get(1)
                .context("failed reading wallet_activity_days inclusive day count")?;
            let has_start_day: i64 = row
                .get(2)
                .context("failed reading wallet_activity_days start-day presence")?;
            let has_end_day: i64 = row
                .get(3)
                .context("failed reading wallet_activity_days end-day presence")?;
            activity_day_summaries.insert(
                wallet_id,
                ObservedWalletActivityDaySummaryRow {
                    inclusive_day_count: inclusive_day_count.max(0) as u32,
                    has_start_day: has_start_day > 0,
                    has_end_day: has_end_day > 0,
                },
            );
        }

        Ok(ObservedWalletActivityDaySummaryPage {
            rows: activity_day_summaries,
            time_budget_exhausted: false,
        })
    }

    pub(super) fn observed_wallet_active_day_counts_from_swaps_in_window_with_budget(
        &self,
        wallet_ids: &[String],
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        deadline: Instant,
    ) -> Result<ObservedWalletActiveDayCountPage> {
        if wallet_ids.is_empty() {
            return Ok(ObservedWalletActiveDayCountPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedWalletActiveDayCountPage {
                counts: HashMap::new(),
                time_budget_exhausted: true,
            });
        }

        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let placeholders = std::iter::repeat_n("?", wallet_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT wallet_id, COUNT(DISTINCT substr(ts, 1, 10))
             FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
             WHERE ts >= ?1
               AND ts <= ?2
               AND wallet_id IN ({placeholders})
             GROUP BY wallet_id"
        );
        let mut params = vec![
            rusqlite::types::Value::from(since.to_rfc3339()),
            rusqlite::types::Value::from(until.to_rfc3339()),
        ];
        params.extend(wallet_ids.iter().cloned().map(rusqlite::types::Value::from));
        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("failed to prepare observed_swaps fallback day-count query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed querying observed_swaps fallback day counts")?;
        let mut counts = HashMap::new();
        loop {
            let next_row = match rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        return Ok(ObservedWalletActiveDayCountPage {
                            counts: HashMap::new(),
                            time_budget_exhausted: true,
                        });
                    }
                    return Err(error)
                        .context("failed iterating observed_swaps fallback day counts");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            let wallet_id: String = row
                .get(0)
                .context("failed reading observed_swaps fallback day-count wallet_id")?;
            let count: i64 = row
                .get(1)
                .context("failed reading observed_swaps fallback day count")?;
            counts.insert(wallet_id, count.max(0) as u32);
        }

        Ok(ObservedWalletActiveDayCountPage {
            counts,
            time_budget_exhausted: false,
        })
    }

    pub(crate) fn sqlite_index_exists(&self, index_name: &str) -> Result<bool> {
        Ok(self
            .conn
            .query_row(
                "SELECT 1
                 FROM sqlite_master
                 WHERE type = 'index'
                   AND name = ?1
                 LIMIT 1",
                params![index_name],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .with_context(|| format!("failed checking sqlite index presence for {index_name}"))?
            .is_some())
    }

    pub fn for_each_observed_sol_leg_swap_in_window_after_cursor_with_budget<F>(
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

        let mut total_rows_seen = 0usize;
        let mut time_budget_exhausted = false;
        let mut page_cursor = cursor.cloned();

        while total_rows_seen < limit && Instant::now() < deadline {
            let page_limit = limit
                .saturating_sub(total_rows_seen)
                .min(OBSERVED_SOL_LEG_CURSOR_QUERY_PAGE_LIMIT);
            let mut next_cursor = page_cursor.clone();
            let page = self
                .for_each_observed_sol_leg_swap_in_window_after_cursor_single_statement_with_budget(
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
}
