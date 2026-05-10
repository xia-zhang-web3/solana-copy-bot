use super::*;

impl SqliteStore {
    pub fn insert_observed_swaps_batch_with_activity_days(
        &self,
        swaps: &[SwapEvent],
    ) -> Result<Vec<bool>> {
        Ok(self
            .insert_observed_swaps_batch_with_activity_days_measured(swaps)?
            .inserted)
    }

    pub fn insert_observed_swaps_batch_with_activity_days_measured(
        &self,
        swaps: &[SwapEvent],
    ) -> Result<ObservedSwapBatchWriteMetrics> {
        if swaps.is_empty() {
            return Ok(ObservedSwapBatchWriteMetrics {
                inserted: Vec::new(),
                observed_swaps_insert_ms: 0,
                wallet_activity_days_upsert_ms: 0,
            });
        }

        self.with_immediate_transaction_retry("observed swap batch write", |conn| {
            let observed_swaps_insert_started = Instant::now();
            let mut stmt = conn
                .prepare_cached(
                    "INSERT OR IGNORE INTO observed_swaps(
                        signature,
                        wallet_id,
                        dex,
                        token_in,
                        token_out,
                        qty_in,
                        qty_out,
                        qty_in_raw,
                        qty_in_decimals,
                        qty_out_raw,
                        qty_out_decimals,
                        slot,
                        ts
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                )
                .context("failed to prepare observed swap batch insert statement")?;

            let mut inserted = Vec::with_capacity(swaps.len());
            let mut activity_rows = Vec::new();
            let mut activity_dedup = std::collections::HashMap::<
                (String, chrono::NaiveDate),
                chrono::DateTime<Utc>,
            >::new();
            for swap in swaps {
                let changed = stmt
                    .execute(params![
                        &swap.signature,
                        &swap.wallet,
                        &swap.dex,
                        &swap.token_in,
                        &swap.token_out,
                        swap.amount_in,
                        swap.amount_out,
                        swap.exact_amounts
                            .as_ref()
                            .map(|value| value.amount_in_raw.as_str()),
                        swap.exact_amounts
                            .as_ref()
                            .map(|value| i64::from(value.amount_in_decimals)),
                        swap.exact_amounts
                            .as_ref()
                            .map(|value| value.amount_out_raw.as_str()),
                        swap.exact_amounts
                            .as_ref()
                            .map(|value| i64::from(value.amount_out_decimals)),
                        swap.slot as i64,
                        swap.ts_utc.to_rfc3339(),
                    ])
                    .context("failed to insert observed swap in batch write")?;
                let was_inserted = changed > 0;
                inserted.push(was_inserted);
                if was_inserted {
                    let key = (swap.wallet.clone(), swap.ts_utc.date_naive());
                    activity_dedup
                        .entry(key)
                        .and_modify(|current| {
                            if swap.ts_utc > *current {
                                *current = swap.ts_utc;
                            }
                        })
                        .or_insert(swap.ts_utc);
                }
            }
            let observed_swaps_insert_ms =
                duration_ms_ceil(observed_swaps_insert_started.elapsed());

            activity_rows.extend(activity_dedup.into_iter().map(
                |((wallet_id, activity_day), last_seen)| WalletActivityDayRow {
                    wallet_id,
                    activity_day,
                    last_seen,
                },
            ));
            let wallet_activity_days_started = Instant::now();
            upsert_wallet_activity_days_on_conn(conn, &activity_rows)?;
            let wallet_activity_days_upsert_ms =
                duration_ms_ceil(wallet_activity_days_started.elapsed());
            Ok(ObservedSwapBatchWriteMetrics {
                inserted,
                observed_swaps_insert_ms,
                wallet_activity_days_upsert_ms,
            })
        })
        .context("failed to insert observed swap batch with activity days")
    }

    pub fn delete_observed_swaps_before(&self, cutoff: DateTime<Utc>) -> Result<usize> {
        self.delete_observed_swaps_before_batched(cutoff, usize::MAX)
            .map(|summary| summary.deleted_rows)
    }

    pub fn delete_observed_swaps_before_batched(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<SqliteBatchedDeleteSummary> {
        let mut summary = SqliteBatchedDeleteSummary::default();
        loop {
            let deleted = self.delete_observed_swaps_before_batch(cutoff, batch_size)?;
            if deleted == 0 {
                break;
            }
            summary.deleted_rows += deleted;
            summary.batches += 1;
        }
        Ok(summary)
    }

    pub fn delete_observed_swaps_before_batch(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<usize> {
        let cutoff_ts = cutoff.to_rfc3339();
        let batch_limit = batch_size.max(1).min(i64::MAX as usize) as i64;
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "DELETE FROM observed_swaps
                 WHERE rowid IN (
                     SELECT rowid
                     FROM observed_swaps
                     WHERE ts < ?1
                     ORDER BY ts ASC, slot ASC, signature ASC
                     LIMIT ?2
                 )",
                params![&cutoff_ts, batch_limit],
            )
        })
        .context("failed to delete observed swap retention slice")
    }

    pub fn checkpoint_wal_truncate(&self) -> Result<(i64, i64, i64)> {
        self.execute_with_retry_result(|conn| {
            let mut stmt = conn.prepare("PRAGMA wal_checkpoint(TRUNCATE)")?;
            stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        })
        .context("failed to checkpoint sqlite wal")
    }

    pub fn checkpoint_wal_passive(&self) -> Result<(i64, i64, i64)> {
        self.execute_with_retry_result(|conn| {
            let mut stmt = conn.prepare("PRAGMA wal_checkpoint(PASSIVE)")?;
            stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        })
        .context("failed to checkpoint sqlite wal in passive mode")
    }

    pub fn load_observed_swaps_since(&self, since: DateTime<Utc>) -> Result<Vec<SwapEvent>> {
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 WHERE ts >= ?1
                 ORDER BY ts ASC, slot ASC",
            )
            .context("failed to prepare observed_swaps load query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339()])
            .context("failed to query observed_swaps")?;

        let mut swaps = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating observed_swaps rows")?
        {
            swaps.push(Self::row_to_swap_event(row)?);
        }

        Ok(swaps)
    }

    pub fn oldest_observed_swap_timestamp(&self) -> Result<Option<DateTime<Utc>>> {
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;
        let raw: Option<String> = self
            .conn
            .query_row("SELECT MIN(ts) FROM observed_swaps", [], |row| row.get(0))
            .optional()
            .context("failed querying oldest observed_swaps timestamp")?
            .flatten();
        parse_optional_rfc3339_utc(raw, "observed_swaps.ts")
    }

    pub fn load_observed_buy_mints_in_window(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<String>> {
        ensure_recent_raw_observed_swaps_timestamps_canonical_utc(&self.conn)?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT token_out
                 FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                 WHERE ts >= ?1
                   AND ts <= ?2
                   AND token_in = ?3
                   AND token_out != ?3
                 ORDER BY token_out ASC",
            )
            .context("failed to prepare observed_swaps distinct buy mint query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339(), until.to_rfc3339(), SOL_MINT,])
            .context("failed to query observed_swaps distinct buy mints")?;

        let mut mints = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating observed_swaps distinct buy mints")?
        {
            mints.push(
                row.get::<_, String>(0)
                    .context("failed reading observed_swaps distinct buy mint")?,
            );
        }

        Ok(mints)
    }
}
