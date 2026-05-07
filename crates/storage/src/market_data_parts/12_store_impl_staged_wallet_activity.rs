use super::*;

impl SqliteStore {
    pub fn observed_wallet_activity_page_for_staged_wallet_ids_in_window_with_budget(
        &self,
        wallet_ids: &[String],
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        max_tx_per_minute: u32,
        deadline: Instant,
    ) -> Result<ObservedWalletActivityPage> {
        let mut page = self.observed_wallet_activity_page_for_wallet_ids_in_window_with_budget(
            wallet_ids,
            since,
            until,
            max_tx_per_minute,
            deadline,
        )?;
        if page.time_budget_exhausted && page.rows_seen == 0 && !wallet_ids.is_empty() {
            page.wallet_id_page_wallets_seen = wallet_ids.len();
            page.wallet_id_page_cursor_after = wallet_ids.last().cloned();
            page.wallet_id_page_wallet_ids = wallet_ids.to_vec();
        }
        Ok(page)
    }

    pub(super) fn load_observed_wallet_activity_wallet_id_page_from_rows(
        &self,
        mut wallet_ids_rows: rusqlite::Rows<'_>,
    ) -> Result<ObservedWalletActivityWalletIdPage> {
        let mut wallet_id_page = ObservedWalletActivityWalletIdPage::default();
        loop {
            let next_row = match wallet_ids_rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        wallet_id_page.time_budget_exhausted = true;
                        return Ok(wallet_id_page);
                    }
                    return Err(error)
                        .context("failed iterating observed wallet activity wallet-id rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            wallet_id_page.wallet_ids.push(
                row.get::<_, String>(0)
                    .context("failed reading observed wallet activity wallet_id")?,
            );
        }
        Ok(wallet_id_page)
    }

    pub(super) fn observed_wallet_activity_wallet_id_query_exhausted_page(
    ) -> ObservedWalletActivityPage {
        ObservedWalletActivityPage {
            rows: Vec::new(),
            rows_seen: 0,
            time_budget_exhausted: true,
            active_day_count_source: None,
            wallet_id_query_exhausted_before_first_page: true,
            ..ObservedWalletActivityPage::default()
        }
    }
}
