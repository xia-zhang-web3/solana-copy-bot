impl SqliteStore {
    pub fn wal_autocheckpoint_pages(&self) -> Result<i64> {
        self.conn
            .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))
            .context("failed to read sqlite wal_autocheckpoint")
    }

    pub fn set_wal_autocheckpoint_pages(&self, pages: i64) -> Result<()> {
        self.conn
            .pragma_update(None, "wal_autocheckpoint", pages)
            .with_context(|| format!("failed to set sqlite wal_autocheckpoint={} pages", pages))?;
        Ok(())
    }

    pub fn wallet_activity_day_coverage_since(
        &self,
        wallet_ids: &[String],
        window_start: DateTime<Utc>,
    ) -> Result<WalletActivityDayCoverageSummary> {
        if wallet_ids.is_empty() {
            return Ok(WalletActivityDayCoverageSummary::default());
        }

        let mut canonical_wallet_ids = wallet_ids.to_vec();
        canonical_wallet_ids.sort();
        canonical_wallet_ids.dedup();

        let day_start = window_start.date_naive();
        let mut summary = WalletActivityDayCoverageSummary::default();
        for chunk in canonical_wallet_ids.chunks(900) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT MIN(activity_day), MAX(activity_day), COUNT(*), COUNT(DISTINCT wallet_id)
                 FROM wallet_activity_days
                 WHERE (
                        activity_day > ?1
                        OR (activity_day = ?1 AND last_seen >= ?2)
                    )
                   AND wallet_id IN ({placeholders})"
            );
            let mut params = vec![
                rusqlite::types::Value::from(day_start.format("%Y-%m-%d").to_string()),
                rusqlite::types::Value::from(window_start.to_rfc3339()),
            ];
            params.extend(chunk.iter().cloned().map(rusqlite::types::Value::from));
            let mut stmt = self
                .conn
                .prepare(&sql)
                .context("failed to prepare wallet_activity_days coverage query")?;
            let (min_day_raw, max_day_raw, row_count, distinct_wallet_count): (
                Option<String>,
                Option<String>,
                i64,
                i64,
            ) = stmt
                .query_row(rusqlite::params_from_iter(params), |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .context("failed querying wallet_activity_days coverage")?;
            if let Some(min_day_raw) = min_day_raw {
                let min_day =
                    NaiveDate::parse_from_str(&min_day_raw, "%Y-%m-%d").with_context(|| {
                        format!("invalid wallet_activity_days min activity_day: {min_day_raw}")
                    })?;
                let min_day_utc = DateTime::<Utc>::from_naive_utc_and_offset(
                    min_day.and_hms_opt(0, 0, 0).expect("midnight utc day"),
                    Utc,
                );
                summary.window_min_day_utc = Some(
                    summary
                        .window_min_day_utc
                        .map(|current| current.min(min_day_utc))
                        .unwrap_or(min_day_utc),
                );
            }
            if let Some(max_day_raw) = max_day_raw {
                let max_day =
                    NaiveDate::parse_from_str(&max_day_raw, "%Y-%m-%d").with_context(|| {
                        format!("invalid wallet_activity_days max activity_day: {max_day_raw}")
                    })?;
                let max_day_utc = DateTime::<Utc>::from_naive_utc_and_offset(
                    max_day.and_hms_opt(0, 0, 0).expect("midnight utc day"),
                    Utc,
                );
                summary.window_max_day_utc = Some(
                    summary
                        .window_max_day_utc
                        .map(|current| current.max(max_day_utc))
                        .unwrap_or(max_day_utc),
                );
            }
            summary.rows_for_wallets = summary
                .rows_for_wallets
                .saturating_add(row_count.max(0) as u64);
            summary.distinct_wallets_for_wallets = summary
                .distinct_wallets_for_wallets
                .saturating_add(distinct_wallet_count.max(0) as u64);
        }

        Ok(summary)
    }

    fn exact_money_cutover_ts_on_conn(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
        let table_exists = conn
            .query_row(
                "SELECT 1
                 FROM sqlite_master
                 WHERE type = 'table'
                   AND name = 'exact_money_cutover_state'
                 LIMIT 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .context("failed checking exact_money_cutover_state presence")?
            .is_some();
        if !table_exists {
            return Ok(None);
        }

        let cutover_ts_raw: Option<String> = conn
            .query_row(
                "SELECT cutover_ts
                 FROM exact_money_cutover_state
                 WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .context("failed loading exact money cutover timestamp")?;
        let Some(cutover_ts_raw) = cutover_ts_raw else {
            return Ok(None);
        };

        let cutover_ts = DateTime::parse_from_rfc3339(&cutover_ts_raw)
            .with_context(|| {
                format!("invalid exact_money_cutover_state.cutover_ts={cutover_ts_raw}")
            })?
            .with_timezone(&Utc);
        Ok(Some(cutover_ts))
    }

    pub fn exact_money_cutover_ts(&self) -> Result<Option<DateTime<Utc>>> {
        Self::exact_money_cutover_ts_on_conn(&self.conn)
    }

    pub fn exact_money_cutover_active_at(&self, ts: DateTime<Utc>) -> Result<bool> {
        Ok(self
            .exact_money_cutover_ts()?
            .map(|cutover_ts| ts >= cutover_ts)
            .unwrap_or(false))
    }

    pub fn upsert_exact_money_cutover_state(
        &self,
        cutover_ts: DateTime<Utc>,
        note: Option<&str>,
    ) -> Result<()> {
        let recorded_ts = Utc::now();
        self.conn
            .execute(
                "INSERT INTO exact_money_cutover_state(id, cutover_ts, recorded_ts, note)
                 VALUES (1, ?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET
                   cutover_ts = excluded.cutover_ts,
                   recorded_ts = excluded.recorded_ts,
                   note = excluded.note",
                params![cutover_ts.to_rfc3339(), recorded_ts.to_rfc3339(), note],
            )
            .context("failed upserting exact_money_cutover_state")?;
        Ok(())
    }
}
