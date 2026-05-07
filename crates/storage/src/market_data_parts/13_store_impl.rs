use super::*;

impl SqliteStore {
    pub(crate) fn observed_wallet_activity_page_for_wallet_ids_in_window_with_budget(
        &self,
        wallet_ids: &[String],
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        max_tx_per_minute: u32,
        deadline: Instant,
    ) -> Result<ObservedWalletActivityPage> {
        if wallet_ids.is_empty() {
            return Ok(ObservedWalletActivityPage::default());
        }

        let placeholders = std::iter::repeat_n("?", wallet_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let since_raw = since.to_rfc3339();
        let until_raw = until.to_rfc3339();

        let mut summaries: HashMap<String, ObservedWalletActivityRow> = HashMap::new();
        let summary_query = format!(
            "SELECT wallet_id, MIN(ts), MAX(ts), COUNT(*)
             FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
             WHERE ts >= ?1
               AND ts <= ?2
               AND wallet_id IN ({placeholders})
             GROUP BY wallet_id
             ORDER BY wallet_id ASC"
        );
        let mut summary_params = vec![since_raw.clone().into(), until_raw.clone().into()];
        summary_params.extend(wallet_ids.iter().cloned().map(rusqlite::types::Value::from));
        let mut summary_stmt = self
            .conn
            .prepare(&summary_query)
            .context("failed to prepare observed wallet activity summary query")?;
        let mut summary_rows = summary_stmt
            .query(rusqlite::params_from_iter(summary_params))
            .context("failed querying observed wallet activity summary rows")?;
        let mut rows_seen = 0usize;
        loop {
            let next_row = match summary_rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        return Ok(ObservedWalletActivityPage {
                            rows: Vec::new(),
                            rows_seen: 0,
                            time_budget_exhausted: true,
                            active_day_count_source: None,
                            ..ObservedWalletActivityPage::default()
                        });
                    }
                    return Err(error)
                        .context("failed iterating observed wallet activity summary rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            let wallet_id: String = row
                .get(0)
                .context("failed reading observed wallet activity summary wallet_id")?;
            let first_seen_raw: String = row
                .get(1)
                .context("failed reading observed wallet activity summary first_seen")?;
            let last_seen_raw: String = row
                .get(2)
                .context("failed reading observed wallet activity summary last_seen")?;
            let trades_raw: i64 = row
                .get(3)
                .context("failed reading observed wallet activity summary trades")?;
            let trades = trades_raw.max(0) as usize;
            rows_seen = rows_seen.saturating_add(trades);
            summaries.insert(
                wallet_id.clone(),
                ObservedWalletActivityRow {
                    wallet_id,
                    first_seen: parse_rfc3339_utc(
                        &first_seen_raw,
                        "observed wallet activity summary first_seen",
                    )?,
                    last_seen: parse_rfc3339_utc(
                        &last_seen_raw,
                        "observed wallet activity summary last_seen",
                    )?,
                    trades,
                    active_day_count: 0,
                    suspicious: false,
                },
            );
        }

        let active_day_summaries = self
            .observed_wallet_activity_day_summaries_in_window_with_budget(
                wallet_ids, since, until, deadline,
            )?;
        let mut active_day_count_source =
            Some(ObservedWalletActivityDayCountSource::WalletActivityDays);
        if active_day_summaries.time_budget_exhausted {
            return Ok(ObservedWalletActivityPage {
                rows: Vec::new(),
                rows_seen: 0,
                time_budget_exhausted: true,
                active_day_count_source: None,
                ..ObservedWalletActivityPage::default()
            });
        }
        let since_day = since.date_naive();
        let until_day = until.date_naive();
        let same_day_window = since_day == until_day;
        let mut active_day_counts = HashMap::new();
        if active_day_summaries.rows.len() != wallet_ids.len() {
            let fallback_active_day_counts = self
                .observed_wallet_active_day_counts_from_swaps_in_window_with_budget(
                    wallet_ids, since, until, deadline,
                )?;
            if fallback_active_day_counts.time_budget_exhausted {
                return Ok(ObservedWalletActivityPage {
                    rows: Vec::new(),
                    rows_seen: 0,
                    time_budget_exhausted: true,
                    active_day_count_source: None,
                    ..ObservedWalletActivityPage::default()
                });
            }
            active_day_counts = fallback_active_day_counts.counts;
            active_day_count_source =
                Some(ObservedWalletActivityDayCountSource::ObservedSwapsFallback);
        } else {
            for wallet_id in wallet_ids {
                let summary = summaries.get(wallet_id).ok_or_else(|| {
                    anyhow!(
                        "missing observed wallet activity summary for wallet {} while loading exact day counts",
                        wallet_id
                    )
                })?;
                let activity_day_summary =
                    active_day_summaries.rows.get(wallet_id).ok_or_else(|| {
                        anyhow!(
                            "missing wallet_activity_days summary for wallet {} in observed wallet activity page",
                            wallet_id
                        )
                    })?;
                let active_day_count = if same_day_window {
                    1
                } else {
                    let mut count = activity_day_summary.inclusive_day_count;
                    if activity_day_summary.has_start_day
                        && summary.first_seen.date_naive() > since_day
                    {
                        count = count.saturating_sub(1);
                    }
                    if activity_day_summary.has_end_day
                        && summary.last_seen.date_naive() < until_day
                    {
                        count = count.saturating_sub(1);
                    }
                    if count == 0 {
                        return Err(anyhow!(
                            "wallet_activity_days summary resolved to zero in-window days for wallet {} despite observed_swaps summary rows",
                            wallet_id
                        ));
                    }
                    count
                };
                active_day_counts.insert(wallet_id.clone(), active_day_count);
            }
        }
        for wallet_id in wallet_ids {
            let active_day_count = active_day_counts.get(wallet_id).copied().ok_or_else(|| {
                anyhow!(
                    "failed loading exact wallet activity day count for wallet {} in observed wallet activity page",
                    wallet_id
                )
            })?;
            if let Some(summary) = summaries.get_mut(wallet_id) {
                summary.active_day_count = active_day_count;
            }
        }

        let max_tx_query = format!(
            "SELECT wallet_id, MAX(tx_count)
             FROM (
                SELECT wallet_id,
                       CAST(strftime('%s', ts) AS INTEGER) / 60 AS minute_bucket,
                       COUNT(*) AS tx_count
                FROM observed_swaps INDEXED BY idx_observed_swaps_wallet_ts
                WHERE ts >= ?1
                  AND ts <= ?2
                  AND wallet_id IN ({placeholders})
                GROUP BY wallet_id, minute_bucket
             )
             GROUP BY wallet_id"
        );
        let mut max_tx_params = vec![since_raw.into(), until_raw.into()];
        max_tx_params.extend(wallet_ids.iter().cloned().map(rusqlite::types::Value::from));
        let mut max_tx_stmt = self
            .conn
            .prepare(&max_tx_query)
            .context("failed to prepare observed wallet activity max-tx query")?;
        let mut max_tx_rows = max_tx_stmt
            .query(rusqlite::params_from_iter(max_tx_params))
            .context("failed querying observed wallet activity max-tx rows")?;
        loop {
            let next_row = match max_tx_rows.next() {
                Ok(row) => row,
                Err(error) => {
                    if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) {
                        return Ok(ObservedWalletActivityPage {
                            rows: Vec::new(),
                            rows_seen: 0,
                            time_budget_exhausted: true,
                            active_day_count_source: None,
                            ..ObservedWalletActivityPage::default()
                        });
                    }
                    return Err(error)
                        .context("failed iterating observed wallet activity max-tx rows");
                }
            };
            let Some(row) = next_row else {
                break;
            };
            let wallet_id: String = row
                .get(0)
                .context("failed reading observed wallet activity max-tx wallet_id")?;
            let max_tx_raw: i64 = row
                .get(1)
                .context("failed reading observed wallet activity max(tx_count)")?;
            if let Some(summary) = summaries.get_mut(&wallet_id) {
                summary.suspicious = (max_tx_raw.max(0) as u32) > max_tx_per_minute.max(1);
            }
        }

        let rows = wallet_ids
            .iter()
            .filter_map(|wallet_id| summaries.remove(wallet_id))
            .collect();
        Ok(ObservedWalletActivityPage {
            rows,
            rows_seen,
            time_budget_exhausted: false,
            active_day_count_source,
            ..ObservedWalletActivityPage::default()
        })
    }
}
