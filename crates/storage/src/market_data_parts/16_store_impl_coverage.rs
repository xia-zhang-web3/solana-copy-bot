use super::*;

impl SqliteStore {
    pub fn observed_swaps_coverage_snapshot(&self) -> Result<ObservedSwapsCoverageSnapshot> {
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;
        let row_count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM observed_swaps", [], |row| row.get(0))
            .context("failed counting observed_swaps rows")?;
        let covered_since_raw: Option<String> = self
            .conn
            .query_row("SELECT MIN(ts) FROM observed_swaps", [], |row| row.get(0))
            .optional()
            .context("failed loading observed_swaps covered_since timestamp")?
            .flatten();
        let covered_since = parse_optional_rfc3339_utc(covered_since_raw, "observed_swaps.ts")?;
        let covered_through_cursor_raw = self
            .conn
            .query_row(
                "SELECT ts, slot, signature
                     FROM observed_swaps
                     ORDER BY ts DESC, slot DESC, signature DESC
                     LIMIT 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get(2)?)),
            )
            .optional()
            .context("failed loading observed_swaps covered_through cursor")?;
        let covered_through_cursor = covered_through_cursor_raw
            .map(
                |(ts_raw, slot_raw, signature)| -> Result<DiscoveryRuntimeCursor> {
                    Ok(DiscoveryRuntimeCursor {
                        ts_utc: parse_rfc3339_utc(&ts_raw, "observed_swaps.ts")?,
                        slot: parse_sqlite_slot(slot_raw, "observed_swaps.slot")?,
                        signature,
                    })
                },
            )
            .transpose()?;
        Ok(ObservedSwapsCoverageSnapshot {
            covered_since,
            covered_through_cursor,
            row_count: row_count.max(0) as usize,
        })
    }

    pub fn observed_swaps_tail_cursor_read_only(&self) -> Result<Option<DiscoveryRuntimeCursor>> {
        if !self.sqlite_table_exists("observed_swaps")? {
            return Ok(None);
        }
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;
        let cursor_raw = self
            .conn
            .query_row(OBSERVED_SWAPS_TAIL_CURSOR_QUERY, [], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .optional()
            .context("failed loading bounded observed_swaps tail cursor")?;
        cursor_raw
            .map(
                |(ts_raw, slot_raw, signature)| -> Result<DiscoveryRuntimeCursor> {
                    Ok(DiscoveryRuntimeCursor {
                        ts_utc: parse_rfc3339_utc(&ts_raw, "observed_swaps tail ts")?,
                        slot: parse_sqlite_slot(slot_raw, "observed_swaps tail slot")?,
                        signature,
                    })
                },
            )
            .transpose()
    }

    pub fn load_observed_swaps_after_cursor_page_read_only(
        &self,
        cursor: &DiscoveryRuntimeCursor,
        limit: usize,
        deadline: Instant,
    ) -> Result<(Vec<SwapEvent>, bool)> {
        if limit == 0 || !self.sqlite_table_exists("observed_swaps")? {
            return Ok((Vec::new(), false));
        }
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;
        if Instant::now() >= deadline {
            return Ok((Vec::new(), true));
        }

        let limit = (limit.min(i64::MAX as usize)) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let mut stmt = match self.conn.prepare(OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY) {
            Ok(stmt) => stmt,
            Err(error) if sqlite_interrupted_after_deadline(&error, deadline) => {
                return Ok((Vec::new(), true));
            }
            Err(error) => {
                return Err(error)
                    .context("failed to prepare bounded observed_swaps after-cursor page query");
            }
        };
        let mut rows = match stmt.query(params![
            cursor.ts_utc.to_rfc3339(),
            cursor.slot as i64,
            cursor.signature.as_str(),
            limit,
        ]) {
            Ok(rows) => rows,
            Err(error) if sqlite_interrupted_after_deadline(&error, deadline) => {
                return Ok((Vec::new(), true));
            }
            Err(error) => {
                return Err(error)
                    .context("failed querying bounded observed_swaps after-cursor page");
            }
        };

        let mut swaps = Vec::new();
        let mut time_budget_exhausted = false;
        loop {
            if Instant::now() >= deadline {
                time_budget_exhausted = true;
                break;
            }
            let next_row = match rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if sqlite_interrupted_after_deadline(&error, deadline) {
                        time_budget_exhausted = true;
                        break;
                    }
                    return Err(error)
                        .context("failed iterating bounded observed_swaps after-cursor rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            swaps.push(Self::row_to_swap_event(row)?);
        }

        Ok((swaps, time_budget_exhausted))
    }

    pub fn observed_swaps_row_count_since(&self, since: DateTime<Utc>) -> Result<u64> {
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;
        let row_count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                     FROM observed_swaps
                     WHERE ts >= ?1",
                params![since.to_rfc3339()],
                |row| row.get(0),
            )
            .context("failed counting observed_swaps rows since window start")?;
        Ok(row_count.max(0) as u64)
    }

    pub fn wallet_activity_days_coverage_snapshot(
        &self,
    ) -> Result<WalletActivityDaysCoverageSnapshot> {
        let (min_day_raw, max_day_raw, row_count): (Option<String>, Option<String>, i64) = self
            .conn
            .query_row(
                "SELECT MIN(activity_day), MAX(activity_day), COUNT(*)
                     FROM wallet_activity_days",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .context("failed querying wallet_activity_days coverage snapshot")?;
        Ok(WalletActivityDaysCoverageSnapshot {
            covered_since_day_utc: parse_optional_day_start_utc(
                min_day_raw,
                "wallet_activity_days.activity_day",
            )?,
            covered_through_day_utc: parse_optional_day_start_utc(
                max_day_raw,
                "wallet_activity_days.activity_day",
            )?,
            row_count: row_count.max(0) as u64,
        })
    }

    pub fn wallet_activity_days_row_count_since(&self, window_start: DateTime<Utc>) -> Result<u64> {
        validate_wallet_activity_days_last_seen_canonical_utc(&self.conn)?;
        let row_count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                     FROM wallet_activity_days
                     WHERE (
                            activity_day > ?1
                            OR (activity_day = ?1 AND last_seen >= ?2)
                        )",
                params![
                    window_start.date_naive().format("%Y-%m-%d").to_string(),
                    window_start.to_rfc3339(),
                ],
                |row| row.get(0),
            )
            .context("failed counting wallet_activity_days rows since window start")?;
        Ok(row_count.max(0) as u64)
    }

    pub fn recent_observed_swap_counts_for_wallets(
        &self,
        since: DateTime<Utc>,
        wallet_ids: &[String],
    ) -> Result<Vec<WalletRecentActivityCountRow>> {
        if wallet_ids.is_empty() {
            return Ok(Vec::new());
        }
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;

        let placeholders = std::iter::repeat_n("?", wallet_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT wallet_id, COUNT(*), MAX(ts)
                 FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
                 WHERE ts >= ?1
                   AND wallet_id IN ({placeholders})
                 GROUP BY wallet_id
                 ORDER BY wallet_id ASC"
        );
        let mut params = vec![rusqlite::types::Value::from(since.to_rfc3339())];
        params.extend(wallet_ids.iter().cloned().map(rusqlite::types::Value::from));
        let mut stmt = self
            .conn
            .prepare(&query)
            .context("failed to prepare recent observed_swaps wallet activity query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed querying recent observed_swaps wallet activity")?;

        let mut summaries = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating recent observed_swaps wallet activity rows")?
        {
            let wallet_id: String = row
                .get(0)
                .context("failed reading recent observed_swaps wallet_id")?;
            let row_count_raw: i64 = row
                .get(1)
                .context("failed reading recent observed_swaps row_count")?;
            let latest_ts_raw: String = row
                .get(2)
                .context("failed reading recent observed_swaps latest_ts")?;
            summaries.push(WalletRecentActivityCountRow {
                wallet_id,
                row_count: row_count_raw.max(0) as usize,
                latest_ts: parse_rfc3339_utc(&latest_ts_raw, "recent observed_swaps latest_ts")?,
            });
        }
        Ok(summaries)
    }
}
