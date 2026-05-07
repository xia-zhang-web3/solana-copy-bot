use super::*;

impl SqliteStore {
    pub fn observed_wallet_activity_page_in_window_with_budget(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        wallet_cursor: Option<&str>,
        wallet_limit: usize,
        max_tx_per_minute: u32,
        deadline: Instant,
    ) -> Result<ObservedWalletActivityPage> {
        if wallet_limit == 0 {
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

        let wallet_limit = wallet_limit.min(900).max(1) as i64;
        let _progress_guard = ProgressHandlerGuard::install(&self.conn, deadline);
        let since_raw = since.to_rfc3339();
        let until_raw = until.to_rfc3339();

        let (wallet_ids_query, wallet_ids_params): (&str, Vec<rusqlite::types::Value>) =
            match wallet_cursor {
                Some(wallet_cursor) => (
                    "SELECT wallet_id
                     FROM (
                        SELECT wallet_id
                        FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
                        WHERE ts >= ?1
                          AND ts <= ?2
                          AND wallet_id > ?3
                        GROUP BY wallet_id
                        ORDER BY wallet_id ASC
                        LIMIT ?4
                     )
                     ORDER BY wallet_id ASC",
                    vec![
                        since_raw.clone().into(),
                        until_raw.clone().into(),
                        wallet_cursor.to_string().into(),
                        wallet_limit.into(),
                    ],
                ),
                None => (
                    "SELECT wallet_id
                     FROM (
                        SELECT wallet_id
                        FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
                        WHERE ts >= ?1
                          AND ts <= ?2
                        GROUP BY wallet_id
                        ORDER BY wallet_id ASC
                        LIMIT ?3
                     )
                     ORDER BY wallet_id ASC",
                    vec![
                        since_raw.clone().into(),
                        until_raw.clone().into(),
                        wallet_limit.into(),
                    ],
                ),
            };

        let mut wallet_ids_stmt = match self.conn.prepare(wallet_ids_query) {
            Ok(stmt) => stmt,
            Err(error) if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) => {
                return Ok(Self::observed_wallet_activity_wallet_id_query_exhausted_page());
            }
            Err(error) => {
                return Err(error)
                    .context("failed to prepare observed wallet activity wallet-id page query");
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
                    .context("failed querying observed wallet activity wallet-id page");
            }
        };
        let wallet_id_page =
            self.load_observed_wallet_activity_wallet_id_page_from_rows(wallet_ids_rows)?;
        if wallet_id_page.time_budget_exhausted {
            return Ok(Self::observed_wallet_activity_wallet_id_query_exhausted_page());
        }
        self.observed_wallet_activity_page_for_wallet_ids_in_window_with_budget(
            &wallet_id_page.wallet_ids,
            since,
            until,
            max_tx_per_minute,
            deadline,
        )
    }
}
