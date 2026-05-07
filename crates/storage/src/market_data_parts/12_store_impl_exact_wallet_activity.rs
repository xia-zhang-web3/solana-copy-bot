use super::*;

impl SqliteStore {
    pub fn observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
        &self,
        exact_wallet_ids: &[String],
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        wallet_cursor: Option<&str>,
        wallet_limit: usize,
        max_tx_per_minute: u32,
        deadline: Instant,
    ) -> Result<ObservedWalletActivityPage> {
        if exact_wallet_ids.is_empty() || wallet_limit == 0 {
            return Ok(ObservedWalletActivityPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedWalletActivityPage {
                rows: Vec::new(),
                rows_seen: 0,
                time_budget_exhausted: true,
                active_day_count_source: None,
                ..ObservedWalletActivityPage::default()
            });
        }

        self.ensure_loaded_observed_wallet_activity_target_wallet_filter(exact_wallet_ids)?;
        let wallet_limit = wallet_limit.min(900).max(1) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let (wallet_ids_query, wallet_ids_params): (&str, Vec<rusqlite::types::Value>) =
            match wallet_cursor {
                Some(wallet_cursor) => (
                    "SELECT wallet_id
                     FROM temp_discovery_replay_candidate_wallets
                     WHERE wallet_id > ?1
                     ORDER BY wallet_id ASC
                     LIMIT ?2",
                    vec![wallet_cursor.to_string().into(), wallet_limit.into()],
                ),
                None => (
                    "SELECT wallet_id
                     FROM temp_discovery_replay_candidate_wallets
                     ORDER BY wallet_id ASC
                     LIMIT ?1",
                    vec![wallet_limit.into()],
                ),
            };
        let mut wallet_ids_stmt = match self.conn.prepare(wallet_ids_query) {
            Ok(stmt) => stmt,
            Err(error) if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) => {
                return Ok(Self::observed_wallet_activity_wallet_id_query_exhausted_page());
            }
            Err(error) => {
                return Err(error).context(
                    "failed to prepare exact observed wallet activity wallet-id page query",
                );
            }
        };
        let wallet_ids_rows = match wallet_ids_stmt
            .query(rusqlite::params_from_iter(wallet_ids_params))
        {
            Ok(rows) => rows,
            Err(error) if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) => {
                return Ok(Self::observed_wallet_activity_wallet_id_query_exhausted_page());
            }
            Err(error) => {
                return Err(error)
                    .context("failed querying exact observed wallet activity wallet-id page");
            }
        };
        let wallet_id_page =
            self.load_observed_wallet_activity_wallet_id_page_from_rows(wallet_ids_rows)?;
        if wallet_id_page.time_budget_exhausted {
            return Ok(Self::observed_wallet_activity_wallet_id_query_exhausted_page());
        }
        #[cfg(test)]
        let forced_pre_row_budget_exhausted =
            EXACT_WALLET_ACTIVITY_PRE_ROW_BUDGET_EXHAUSTION_HOOK.with(|slot| *slot.borrow());
        #[cfg(not(test))]
        let forced_pre_row_budget_exhausted = false;
        if forced_pre_row_budget_exhausted {
            return Ok(ObservedWalletActivityPage {
                rows: Vec::new(),
                rows_seen: 0,
                time_budget_exhausted: true,
                active_day_count_source: None,
                wallet_id_query_exhausted_before_first_page: false,
                wallet_id_page_wallets_seen: wallet_id_page.wallet_ids.len(),
                wallet_id_page_cursor_after: wallet_id_page.wallet_ids.last().cloned(),
                wallet_id_page_wallet_ids: wallet_id_page.wallet_ids,
            });
        }
        let mut page = self.observed_wallet_activity_page_for_wallet_ids_in_window_with_budget(
            &wallet_id_page.wallet_ids,
            since,
            until,
            max_tx_per_minute,
            deadline,
        )?;
        if page.time_budget_exhausted
            && page.rows_seen == 0
            && !wallet_id_page.wallet_ids.is_empty()
        {
            page.wallet_id_page_wallets_seen = wallet_id_page.wallet_ids.len();
            page.wallet_id_page_cursor_after = wallet_id_page.wallet_ids.last().cloned();
            page.wallet_id_page_wallet_ids = wallet_id_page.wallet_ids;
        }
        Ok(page)
    }
}
