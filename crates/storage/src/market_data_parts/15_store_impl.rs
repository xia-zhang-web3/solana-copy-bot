use super::*;

impl SqliteStore {
    pub(crate) fn for_each_observed_sol_leg_swap_in_window_after_cursor_single_statement_for_loaded_target_buy_mint_filter_with_budget<
        F,
    >(
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
                       AND (
                            (token_in = '{SOL_MINT}'
                                AND token_out IN (
                                    SELECT mint FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}
                                ))
                         OR (token_out = '{SOL_MINT}'
                                AND token_in IN (
                                    SELECT mint FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}
                                ))
                       )
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
                       AND (
                            (token_in = '{SOL_MINT}'
                                AND token_out IN (
                                    SELECT mint FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}
                                ))
                         OR (token_out = '{SOL_MINT}'
                                AND token_in IN (
                                    SELECT mint FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}
                                ))
                       )
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
            .context("failed to prepare observed_swaps exact-target SOL-leg window cursor query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed to query observed_swaps exact-target SOL-leg window cursor")?;

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
                    return Err(error).context(
                        "failed iterating observed_swaps exact-target SOL-leg window cursor rows",
                    );
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

    pub fn load_observed_buy_mints_in_window_after_token_with_budget(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        token_out_after: Option<&str>,
        limit: usize,
        deadline: Instant,
    ) -> Result<ObservedBuyMintPage> {
        self.load_observed_buy_mints_in_time_bounds_after_token_with_budget(
            since,
            true,
            until,
            true,
            token_out_after,
            None,
            limit,
            deadline,
        )
    }

    pub fn load_observed_buy_mints_in_time_bounds_after_token_with_budget(
        &self,
        since: DateTime<Utc>,
        since_inclusive: bool,
        until: DateTime<Utc>,
        until_inclusive: bool,
        token_out_after: Option<&str>,
        token_out_at_most: Option<&str>,
        limit: usize,
        deadline: Instant,
    ) -> Result<ObservedBuyMintPage> {
        if limit == 0 {
            return Ok(ObservedBuyMintPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedBuyMintPage {
                mints: Vec::new(),
                time_budget_exhausted: true,
            });
        }

        let limit = (limit.min(i64::MAX as usize)) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let since_op = if since_inclusive { ">=" } else { ">" };
        let until_op = if until_inclusive { "<=" } else { "<" };
        let mut params: Vec<rusqlite::types::Value> = vec![
            SOL_MINT.to_string().into(),
            since.to_rfc3339().into(),
            until.to_rfc3339().into(),
        ];
        // Candidate discovery is used by stale expired-head/new-tail reconciliation.
        // Those paths operate on narrow time windows, so a time-first access path
        // avoids crawling token-order space just to find the next touched mints.
        let mut query = format!(
            "SELECT DISTINCT token_out
             FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_ts
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
        query.push_str(" ORDER BY token_out ASC");
        query.push_str(&format!(" LIMIT ?{next_param}"));
        params.push(limit.into());
        let mut stmt = self
            .conn
            .prepare(&query)
            .context("failed to prepare observed_swaps distinct buy mint page query")?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params))
            .context("failed to query observed_swaps distinct buy mint page")?;

        let mut mints = Vec::new();
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
                        .context("failed iterating observed_swaps distinct buy mint page rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            mints.push(
                row.get::<_, String>(0)
                    .context("failed reading observed_swaps distinct buy mint page row")?,
            );
        }

        Ok(ObservedBuyMintPage {
            mints,
            time_budget_exhausted,
        })
    }
}
