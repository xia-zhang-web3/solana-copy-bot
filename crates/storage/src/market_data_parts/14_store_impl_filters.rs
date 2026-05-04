impl SqliteStore {
    fn observed_sol_leg_cursor_access_path(&self) -> Result<ObservedSolLegCursorAccessPath> {
        Ok(
            if self.sqlite_index_exists("idx_observed_swaps_sol_leg_ts_slot_signature")? {
                ObservedSolLegCursorAccessPath::SolLegPartialIndex
            } else {
                ObservedSolLegCursorAccessPath::TsCursorFallback
            },
        )
    }

    fn ensure_observed_sol_leg_target_buy_mint_filter_table(&self) -> Result<()> {
        self.conn
            .execute_batch(&format!(
                "CREATE TEMP TABLE IF NOT EXISTS {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE} (
                    mint TEXT PRIMARY KEY
                ) WITHOUT ROWID;"
            ))
            .context("failed ensuring temporary observed SOL-leg target-mint filter table exists")
    }

    fn ensure_observed_sol_leg_target_buy_mint_filter_meta_table(&self) -> Result<()> {
        self.conn
            .execute_batch(&format!(
                "CREATE TEMP TABLE IF NOT EXISTS {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_META_TABLE} (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    fingerprint TEXT NOT NULL,
                    mint_count INTEGER NOT NULL
                ) WITHOUT ROWID;"
            ))
            .context(
                "failed ensuring temporary observed SOL-leg target-mint filter metadata table exists",
            )
    }

    fn observed_sol_leg_target_buy_mint_filter_fingerprint(target_buy_mints: &[String]) -> String {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        target_buy_mints.len().hash(&mut hasher);
        for mint in target_buy_mints {
            mint.hash(&mut hasher);
        }
        format!("{:016x}", hasher.finish())
    }

    fn ensure_loaded_observed_sol_leg_target_buy_mint_filter(
        &self,
        target_buy_mints: &[String],
    ) -> Result<()> {
        self.ensure_observed_sol_leg_target_buy_mint_filter_table()?;
        self.ensure_observed_sol_leg_target_buy_mint_filter_meta_table()?;

        let fingerprint =
            Self::observed_sol_leg_target_buy_mint_filter_fingerprint(target_buy_mints);
        let expected_count = target_buy_mints.len() as i64;
        let loaded = self
            .conn
            .query_row(
                &format!(
                    "SELECT fingerprint, mint_count
                     FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_META_TABLE}
                     WHERE id = 1"
                ),
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .context("failed reading temporary observed SOL-leg target-mint filter metadata row")?;
        let loaded_count = self
            .conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}"),
                [],
                |row| row.get::<_, i64>(0),
            )
            .context("failed counting temporary observed SOL-leg target-mint filter rows")?;
        if loaded
            .as_ref()
            .is_some_and(|(loaded_fingerprint, loaded_mint_count)| {
                loaded_fingerprint == &fingerprint
                    && *loaded_mint_count == expected_count
                    && loaded_count == expected_count
            })
        {
            return Ok(());
        }

        self.replace_observed_sol_leg_target_buy_mint_filter(target_buy_mints)?;
        self.conn
            .execute(
                &format!(
                    "INSERT INTO {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_META_TABLE}(
                        id,
                        fingerprint,
                        mint_count
                    ) VALUES (1, ?1, ?2)
                    ON CONFLICT(id) DO UPDATE SET
                        fingerprint = excluded.fingerprint,
                        mint_count = excluded.mint_count"
                ),
                params![fingerprint, expected_count],
            )
            .context(
                "failed persisting temporary observed SOL-leg target-mint filter metadata row",
            )?;
        Ok(())
    }

    fn replace_observed_sol_leg_target_buy_mint_filter(
        &self,
        target_buy_mints: &[String],
    ) -> Result<()> {
        self.ensure_observed_sol_leg_target_buy_mint_filter_table()?;
        self.conn
            .execute(
                &format!("DELETE FROM {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}"),
                [],
            )
            .context("failed clearing temporary observed SOL-leg target-mint filter table")?;
        let mut insert = self
            .conn
            .prepare_cached(&format!(
                "INSERT OR IGNORE INTO {OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE}(mint)
                 VALUES (?1)"
            ))
            .context("failed preparing temporary observed SOL-leg target-mint filter insert")?;
        for mint in target_buy_mints {
            insert
                .execute(params![mint])
                .with_context(|| format!("failed inserting target buy mint into temporary observed SOL-leg filter table: {mint}"))?;
        }
        Ok(())
    }

    fn ensure_observed_wallet_activity_target_wallet_filter_table(&self) -> Result<()> {
        self.conn
            .execute_batch(&format!(
                "CREATE TEMP TABLE IF NOT EXISTS {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_TABLE} (
                    wallet_id TEXT PRIMARY KEY
                ) WITHOUT ROWID;"
            ))
            .context(
                "failed ensuring temporary observed wallet activity target-wallet filter table exists",
            )
    }

    fn ensure_observed_wallet_activity_target_wallet_filter_meta_table(&self) -> Result<()> {
        self.conn
            .execute_batch(&format!(
                "CREATE TEMP TABLE IF NOT EXISTS {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_META_TABLE} (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    fingerprint TEXT NOT NULL,
                    wallet_count INTEGER NOT NULL
                ) WITHOUT ROWID;"
            ))
            .context(
                "failed ensuring temporary observed wallet activity target-wallet metadata table exists",
            )
    }

    fn canonical_observed_wallet_activity_target_wallet_ids(wallet_ids: &[String]) -> Vec<String> {
        let mut canonical = wallet_ids.to_vec();
        canonical.sort();
        canonical.dedup();
        canonical
    }

    fn observed_wallet_activity_target_wallet_filter_fingerprint(wallet_ids: &[String]) -> String {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        wallet_ids.len().hash(&mut hasher);
        for wallet_id in wallet_ids {
            wallet_id.hash(&mut hasher);
        }
        format!("{:016x}", hasher.finish())
    }

    fn ensure_loaded_observed_wallet_activity_target_wallet_filter(
        &self,
        wallet_ids: &[String],
    ) -> Result<()> {
        self.ensure_observed_wallet_activity_target_wallet_filter_table()?;
        self.ensure_observed_wallet_activity_target_wallet_filter_meta_table()?;

        let canonical_wallet_ids =
            Self::canonical_observed_wallet_activity_target_wallet_ids(wallet_ids);
        let fingerprint =
            Self::observed_wallet_activity_target_wallet_filter_fingerprint(&canonical_wallet_ids);
        let expected_count = canonical_wallet_ids.len() as i64;
        let loaded = self
            .conn
            .query_row(
                &format!(
                    "SELECT fingerprint, wallet_count
                     FROM {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_META_TABLE}
                     WHERE id = 1"
                ),
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .context(
                "failed reading temporary observed wallet activity target-wallet metadata row",
            )?;
        let loaded_count = self
            .conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_TABLE}"
                ),
                [],
                |row| row.get::<_, i64>(0),
            )
            .context("failed counting temporary observed wallet activity target-wallet rows")?;
        if loaded
            .as_ref()
            .is_some_and(|(loaded_fingerprint, loaded_wallet_count)| {
                loaded_fingerprint == &fingerprint
                    && *loaded_wallet_count == expected_count
                    && loaded_count == expected_count
            })
        {
            return Ok(());
        }

        self.replace_observed_wallet_activity_target_wallet_filter(&canonical_wallet_ids)?;
        self.conn
            .execute(
                &format!(
                    "INSERT INTO {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_META_TABLE}(
                        id,
                        fingerprint,
                        wallet_count
                    ) VALUES (1, ?1, ?2)
                    ON CONFLICT(id) DO UPDATE SET
                        fingerprint = excluded.fingerprint,
                        wallet_count = excluded.wallet_count"
                ),
                params![fingerprint, expected_count],
            )
            .context(
                "failed persisting temporary observed wallet activity target-wallet metadata row",
            )?;
        Ok(())
    }

    fn replace_observed_wallet_activity_target_wallet_filter(
        &self,
        wallet_ids: &[String],
    ) -> Result<()> {
        self.ensure_observed_wallet_activity_target_wallet_filter_table()?;
        self.conn
            .execute(
                &format!("DELETE FROM {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_TABLE}"),
                [],
            )
            .context("failed clearing temporary observed wallet activity target-wallet table")?;
        let mut insert = self
            .conn
            .prepare_cached(&format!(
                "INSERT OR IGNORE INTO {OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_TABLE}(wallet_id)
                 VALUES (?1)"
            ))
            .context("failed preparing temporary observed wallet activity target-wallet insert")?;
        for wallet_id in wallet_ids {
            insert.execute(params![wallet_id]).with_context(|| {
                format!(
                    "failed inserting wallet into temporary observed wallet activity filter table: {wallet_id}"
                )
            })?;
        }
        Ok(())
    }
}
