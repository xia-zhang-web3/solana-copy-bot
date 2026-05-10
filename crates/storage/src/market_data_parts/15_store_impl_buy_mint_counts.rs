use super::*;

impl SqliteStore {
    pub fn load_observed_buy_mint_counts_in_window_after_token_with_budget(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        token_out_after: Option<&str>,
        token_out_at_most: Option<&str>,
        limit: usize,
        deadline: Instant,
    ) -> Result<ObservedBuyMintCountPage> {
        self.load_observed_buy_mint_counts_in_time_bounds_after_token_with_budget(
            since,
            true,
            until,
            true,
            token_out_after,
            token_out_at_most,
            limit,
            deadline,
        )
    }

    pub fn load_observed_buy_mint_counts_in_time_bounds_after_token_with_budget(
        &self,
        since: DateTime<Utc>,
        since_inclusive: bool,
        until: DateTime<Utc>,
        until_inclusive: bool,
        token_out_after: Option<&str>,
        token_out_at_most: Option<&str>,
        limit: usize,
        deadline: Instant,
    ) -> Result<ObservedBuyMintCountPage> {
        if limit == 0 {
            return Ok(ObservedBuyMintCountPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedBuyMintCountPage {
                rows: Vec::new(),
                time_budget_exhausted: true,
            });
        }
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;

        let limit = (limit.min(i64::MAX as usize)) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let since_op = if since_inclusive { ">=" } else { ">" };
        let until_op = if until_inclusive { "<=" } else { "<" };
        let mut params: Vec<rusqlite::types::Value> = vec![
            SOL_MINT.to_string().into(),
            since.to_rfc3339().into(),
            until.to_rfc3339().into(),
        ];
        let mut query = format!(
            "SELECT token_out, COUNT(*)
                 FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                 WHERE token_in = ?1
                   AND token_out <> ?1
                   AND ts {since_op} ?2
                   AND ts {until_op} ?3"
        );
        let mut next_param = 4usize;
        if let Some(token_out_after) = token_out_after {
            query.push_str(&format!(" AND token_out > ?{next_param}"));
            params.push(token_out_after.to_string().into());
            next_param = next_param.saturating_add(1);
        }
        if let Some(token_out_at_most) = token_out_at_most {
            query.push_str(&format!(" AND token_out <= ?{next_param}"));
            params.push(token_out_at_most.to_string().into());
            next_param = next_param.saturating_add(1);
        }
        query.push_str(" GROUP BY token_out ORDER BY token_out ASC");
        query.push_str(&format!(" LIMIT ?{next_param}"));
        params.push(limit.into());

        let mut stmt = match self.conn.prepare(&query) {
            Ok(stmt) => stmt,
            Err(error) if sqlite_interrupted_after_deadline(&error, deadline) => {
                return Ok(ObservedBuyMintCountPage {
                    rows: Vec::new(),
                    time_budget_exhausted: true,
                })
            }
            Err(error) => {
                return Err(error)
                    .context("failed to prepare observed_swaps grouped buy mint count page query")
            }
        };
        let mut rows = match stmt.query(rusqlite::params_from_iter(params)) {
            Ok(rows) => rows,
            Err(error) if sqlite_interrupted_after_deadline(&error, deadline) => {
                return Ok(ObservedBuyMintCountPage {
                    rows: Vec::new(),
                    time_budget_exhausted: true,
                })
            }
            Err(error) => {
                return Err(error)
                    .context("failed to query observed_swaps grouped buy mint count page")
            }
        };

        let mut mint_rows = Vec::new();
        let mut time_budget_exhausted = false;
        loop {
            let next_row = match rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if sqlite_interrupted_after_deadline(&error, deadline) {
                        time_budget_exhausted = true;
                        break;
                    }
                    return Err(error)
                        .context("failed iterating observed_swaps grouped buy mint count rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            mint_rows.push(ObservedBuyMintCountRow {
                mint: row
                    .get::<_, String>(0)
                    .context("failed reading observed_swaps grouped buy mint token")?,
                buy_count: row
                    .get::<_, i64>(1)
                    .context("failed reading observed_swaps grouped buy mint count")?
                    .max(0) as usize,
            });
        }

        Ok(ObservedBuyMintCountPage {
            rows: mint_rows,
            time_budget_exhausted,
        })
    }

    pub fn count_observed_buy_mints_in_window_up_to_token_with_budget(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        token_out_inclusive: &str,
        deadline: Instant,
    ) -> Result<ObservedBuyMintCount> {
        if Instant::now() >= deadline {
            return Ok(ObservedBuyMintCount {
                count: 0,
                time_budget_exhausted: true,
            });
        }
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;

        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        match self.conn.query_row(
            "SELECT COUNT(DISTINCT token_out)
                 FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                 WHERE token_in = ?1
                   AND token_out <> ?1
                   AND ts >= ?2
                   AND ts <= ?3
                   AND token_out <= ?4",
            params![
                SOL_MINT,
                since.to_rfc3339(),
                until.to_rfc3339(),
                token_out_inclusive,
            ],
            |row| row.get::<_, i64>(0),
        ) {
            Ok(count) => Ok(ObservedBuyMintCount {
                count: count.max(0) as usize,
                time_budget_exhausted: false,
            }),
            Err(error) if sqlite_interrupted_after_deadline(&error, deadline) => {
                Ok(ObservedBuyMintCount {
                    count: 0,
                    time_budget_exhausted: true,
                })
            }
            Err(error) => {
                Err(error).context("failed counting observed_swaps distinct buy mints up to token")
            }
        }
    }

    pub fn count_observed_buy_mint_occurrences_in_time_bounds_with_budget(
        &self,
        since: DateTime<Utc>,
        since_inclusive: bool,
        until: DateTime<Utc>,
        until_inclusive: bool,
        token_out: &str,
        deadline: Instant,
    ) -> Result<ObservedBuyMintOccurrenceCount> {
        if Instant::now() >= deadline {
            return Ok(ObservedBuyMintOccurrenceCount {
                buy_count: 0,
                time_budget_exhausted: true,
            });
        }
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;

        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let since_op = if since_inclusive { ">=" } else { ">" };
        let until_op = if until_inclusive { "<=" } else { "<" };
        let query = format!(
            "SELECT COUNT(*)
                 FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                 WHERE token_in = ?1
                   AND token_out = ?2
                   AND ts {since_op} ?3
                   AND ts {until_op} ?4"
        );
        match self.conn.query_row(
            &query,
            params![SOL_MINT, token_out, since.to_rfc3339(), until.to_rfc3339(),],
            |row| row.get::<_, i64>(0),
        ) {
            Ok(count) => Ok(ObservedBuyMintOccurrenceCount {
                buy_count: count.max(0) as usize,
                time_budget_exhausted: false,
            }),
            Err(error) if sqlite_interrupted_after_deadline(&error, deadline) => {
                Ok(ObservedBuyMintOccurrenceCount {
                    buy_count: 0,
                    time_budget_exhausted: true,
                })
            }
            Err(error) => Err(error)
                .context("failed counting observed_swaps buy mint occurrences for exact token"),
        }
    }

    pub fn load_observed_buy_mint_counts_for_exact_tokens_in_time_bounds_with_budget(
        &self,
        since: DateTime<Utc>,
        since_inclusive: bool,
        until: DateTime<Utc>,
        until_inclusive: bool,
        token_outs: &[String],
        deadline: Instant,
    ) -> Result<ObservedBuyMintCountPage> {
        if token_outs.is_empty() {
            return Ok(ObservedBuyMintCountPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedBuyMintCountPage {
                rows: Vec::new(),
                time_budget_exhausted: true,
            });
        }
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;

        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let since_op = if since_inclusive { ">=" } else { ">" };
        let until_op = if until_inclusive { "<=" } else { "<" };
        let mut params: Vec<rusqlite::types::Value> = vec![
            SOL_MINT.to_string().into(),
            since.to_rfc3339().into(),
            until.to_rfc3339().into(),
        ];
        let mut token_placeholders = Vec::with_capacity(token_outs.len());
        for token_out in token_outs {
            let placeholder = format!("?{}", params.len() + 1);
            token_placeholders.push(placeholder);
            params.push(token_out.clone().into());
        }
        let query = format!(
            "SELECT token_out, COUNT(*)
                 FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                 WHERE token_in = ?1
                   AND token_out <> ?1
                   AND ts {since_op} ?2
                   AND ts {until_op} ?3
                   AND token_out IN ({})
                 GROUP BY token_out
                 ORDER BY token_out ASC",
            token_placeholders.join(", ")
        );
        let mut stmt = match self.conn.prepare(&query) {
            Ok(stmt) => stmt,
            Err(error) if sqlite_interrupted_after_deadline(&error, deadline) => {
                return Ok(ObservedBuyMintCountPage {
                    rows: Vec::new(),
                    time_budget_exhausted: true,
                })
            }
            Err(error) => {
                return Err(error)
                    .context("failed to prepare exact observed_swaps buy mint batch count query")
            }
        };
        let mut rows = match stmt.query(rusqlite::params_from_iter(params)) {
            Ok(rows) => rows,
            Err(error) if sqlite_interrupted_after_deadline(&error, deadline) => {
                return Ok(ObservedBuyMintCountPage {
                    rows: Vec::new(),
                    time_budget_exhausted: true,
                })
            }
            Err(error) => {
                return Err(error)
                    .context("failed to query exact observed_swaps buy mint batch count page")
            }
        };

        let mut mint_rows = Vec::new();
        let mut time_budget_exhausted = false;
        loop {
            let next_row = match rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if sqlite_interrupted_after_deadline(&error, deadline) {
                        time_budget_exhausted = true;
                        break;
                    }
                    return Err(error).context(
                        "failed iterating exact observed_swaps buy mint batch count rows",
                    );
                }
            };
            let Some(row) = next_row else {
                break;
            };
            mint_rows.push(ObservedBuyMintCountRow {
                mint: row
                    .get::<_, String>(0)
                    .context("failed reading exact observed_swaps buy mint batch token")?,
                buy_count: row
                    .get::<_, i64>(1)
                    .context("failed reading exact observed_swaps buy mint batch count")?
                    .max(0) as usize,
            });
        }

        Ok(ObservedBuyMintCountPage {
            rows: mint_rows,
            time_budget_exhausted,
        })
    }
}
