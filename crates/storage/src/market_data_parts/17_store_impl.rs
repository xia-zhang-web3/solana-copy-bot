impl SqliteStore {
    pub fn list_unique_sol_buy_mints_since(&self, since: DateTime<Utc>) -> Result<HashSet<String>> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT token_out
                 FROM observed_swaps
                 WHERE ts >= ?1
                   AND token_in = ?2
                   AND token_out <> ?2",
            )
            .context("failed to prepare unique sol-buy mints query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339(), SOL_MINT])
            .context("failed to query unique sol-buy mints")?;

        let mut out = HashSet::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating unique sol-buy mints rows")?
        {
            let mint: String = row
                .get(0)
                .context("failed reading observed_swaps.token_out")?;
            out.insert(mint);
        }
        Ok(out)
    }

    fn ensure_discovery_runtime_state_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS discovery_runtime_state (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    cursor_ts TEXT NOT NULL,
                    cursor_slot INTEGER NOT NULL,
                    cursor_signature TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )",
            )
            .context("failed to ensure discovery_runtime_state table exists")?;
        Ok(())
    }

    fn ensure_discovery_persisted_rebuild_state_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS discovery_persisted_rebuild_state (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    phase TEXT NOT NULL,
                    window_start TEXT NOT NULL,
                    horizon_end TEXT NOT NULL,
                    metrics_window_start TEXT NOT NULL,
                    phase_cursor_ts TEXT,
                    phase_cursor_slot INTEGER,
                    phase_cursor_signature TEXT,
                    prepass_rows_processed INTEGER NOT NULL DEFAULT 0,
                    prepass_pages_processed INTEGER NOT NULL DEFAULT 0,
                    replay_rows_processed INTEGER NOT NULL DEFAULT 0,
                    replay_pages_processed INTEGER NOT NULL DEFAULT 0,
                    chunks_completed INTEGER NOT NULL DEFAULT 0,
                    state_json TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )",
            )
            .context("failed to ensure discovery_persisted_rebuild_state table exists")?;
        Ok(())
    }

    pub(crate) fn row_to_swap_event(row: &rusqlite::Row<'_>) -> Result<SwapEvent> {
        let ts_raw: String = row.get(8).context("failed reading observed_swaps.ts")?;
        let ts_utc = DateTime::parse_from_rfc3339(&ts_raw)
            .map(|dt| dt.with_timezone(&Utc))
            .with_context(|| format!("invalid observed_swaps.ts rfc3339 value: {ts_raw}"))?;
        let slot_raw: i64 = row.get(7).context("failed reading observed_swaps.slot")?;
        let slot = if slot_raw < 0 { 0 } else { slot_raw as u64 };
        let exact_amounts = Self::read_exact_swap_amounts(row)?;

        Ok(SwapEvent {
            signature: row
                .get(0)
                .context("failed reading observed_swaps.signature")?,
            wallet: row
                .get(1)
                .context("failed reading observed_swaps.wallet_id")?,
            dex: row.get(2).context("failed reading observed_swaps.dex")?,
            token_in: row
                .get(3)
                .context("failed reading observed_swaps.token_in")?,
            token_out: row
                .get(4)
                .context("failed reading observed_swaps.token_out")?,
            amount_in: row.get(5).context("failed reading observed_swaps.qty_in")?,
            amount_out: row
                .get(6)
                .context("failed reading observed_swaps.qty_out")?,
            slot,
            ts_utc,
            exact_amounts,
        })
    }

    fn read_exact_swap_amounts(row: &rusqlite::Row<'_>) -> Result<Option<ExactSwapAmounts>> {
        let amount_in_raw: Option<String> = row
            .get(9)
            .context("failed reading observed_swaps.qty_in_raw")?;
        let amount_in_decimals_raw: Option<i64> = row
            .get(10)
            .context("failed reading observed_swaps.qty_in_decimals")?;
        let amount_out_raw: Option<String> = row
            .get(11)
            .context("failed reading observed_swaps.qty_out_raw")?;
        let amount_out_decimals_raw: Option<i64> = row
            .get(12)
            .context("failed reading observed_swaps.qty_out_decimals")?;

        match (
            amount_in_raw,
            amount_in_decimals_raw,
            amount_out_raw,
            amount_out_decimals_raw,
        ) {
            (
                Some(amount_in_raw),
                Some(amount_in_decimals_raw),
                Some(amount_out_raw),
                Some(amount_out_decimals_raw),
            ) => {
                let amount_in_decimals =
                    u8::try_from(amount_in_decimals_raw).with_context(|| {
                        format!(
                            "invalid observed_swaps.qty_in_decimals value: {amount_in_decimals_raw}"
                        )
                    })?;
                let amount_out_decimals =
                    u8::try_from(amount_out_decimals_raw).with_context(|| {
                        format!(
                            "invalid observed_swaps.qty_out_decimals value: {amount_out_decimals_raw}"
                        )
                    })?;
                Ok(Some(ExactSwapAmounts {
                    amount_in_raw,
                    amount_in_decimals,
                    amount_out_raw,
                    amount_out_decimals,
                }))
            }
            (None, None, None, None) => Ok(None),
            _ => Err(anyhow!(
                "observed_swaps exact amount columns must be fully populated or fully NULL"
            )),
        }
    }

}
