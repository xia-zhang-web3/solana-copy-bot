impl SqliteStore {
    pub fn set_discovery_scoring_covered_since(&self, covered_since: DateTime<Utc>) -> Result<()> {
        self.upsert_discovery_scoring_state_ts("covered_since_ts", covered_since)
    }

    pub fn set_discovery_scoring_backfill_progress(
        &self,
        start_ts: DateTime<Utc>,
        cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring backfill progress update",
            |conn| {
                let now = Utc::now().to_rfc3339();
                clear_discovery_scoring_seed_boundary_install_marker_on_conn(conn)?;
                upsert_discovery_scoring_backfill_progress_on_conn(conn, start_ts, cursor, &now)?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn set_discovery_scoring_materialization_gap_cursor(
        &self,
        cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        let existing = self.load_discovery_scoring_materialization_gap_cursor()?;
        if existing
            .as_ref()
            .is_some_and(|current| cmp_cursor_order(current, cursor) != Ordering::Greater)
        {
            return Ok(());
        }
        self.with_immediate_transaction_retry(
            "discovery scoring materialization gap update",
            |conn| {
                let now = Utc::now().to_rfc3339();
                clear_discovery_scoring_materialization_gap_repair_target_on_conn(conn)?;
                upsert_discovery_scoring_materialization_gap_cursor_on_conn(conn, cursor, &now)?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn set_discovery_scoring_materialization_gap_repair_target(
        &self,
        gap_cursor: &DiscoveryRuntimeCursor,
        target_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring materialization gap repair target update",
            |conn| {
                let now = Utc::now().to_rfc3339();
                upsert_discovery_scoring_materialization_gap_repair_target_on_conn(
                    conn,
                    gap_cursor,
                    target_cursor,
                    &now,
                )?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn clear_discovery_scoring_materialization_gap_repair_target(&self) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring materialization gap repair target clear",
            |conn| {
                clear_discovery_scoring_materialization_gap_repair_target_on_conn(conn)?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn set_discovery_scoring_covered_through_cursor(
        &self,
        cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        let existing = self.load_discovery_scoring_covered_through_cursor()?;
        if existing
            .as_ref()
            .is_some_and(|current| cmp_cursor_order(current, cursor) != Ordering::Less)
        {
            return Ok(());
        }
        self.with_immediate_transaction_retry(
            "discovery scoring covered_through cursor update",
            |conn| {
                let now = Utc::now().to_rfc3339();
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('covered_through_ts', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![cursor.ts_utc.to_rfc3339(), &now],
                )
                .context("failed upserting discovery_scoring_state.covered_through_ts")?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('covered_through_slot', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![cursor.slot.to_string(), &now],
                )
                .context("failed upserting discovery_scoring_state.covered_through_slot")?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('covered_through_signature', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![&cursor.signature, &now],
                )
                .context("failed upserting discovery_scoring_state.covered_through_signature")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn clear_discovery_scoring_materialization_gap_if_cursor_observed(
        &self,
        observed_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<()> {
        let Some(gap_cursor) = self.load_discovery_scoring_materialization_gap_cursor()? else {
            return Ok(());
        };
        if cmp_cursor_order(observed_cursor, &gap_cursor) != Ordering::Equal {
            return Ok(());
        }
        self.with_immediate_transaction_retry(
            "discovery scoring materialization gap clear",
            |conn| {
                conn.execute(
                    "DELETE FROM discovery_scoring_state
                 WHERE state_key IN (
                    'materialization_gap_since_ts',
                    'materialization_gap_since_slot',
                    'materialization_gap_since_signature',
                    'materialization_gap_repair_gap_ts',
                    'materialization_gap_repair_gap_slot',
                    'materialization_gap_repair_gap_signature',
                    'materialization_gap_repair_target_ts',
                    'materialization_gap_repair_target_slot',
                    'materialization_gap_repair_target_signature'
                 )",
                    [],
                )
                .context("failed clearing discovery_scoring_state.materialization_gap cursor")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn set_discovery_scoring_backfill_source_protection(
        &self,
        protect_since: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring backfill source protection",
            |conn| {
                let now = Utc::now().to_rfc3339();
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('backfill_protect_since_ts', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![protect_since.to_rfc3339(), &now],
                )
                .context("failed upserting discovery_scoring_state.backfill_protect_since_ts")?;
                conn.execute(
                    "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
                 VALUES ('backfill_protect_expires_at', ?1, ?2)
                 ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at",
                    params![expires_at.to_rfc3339(), &now],
                )
                .context("failed upserting discovery_scoring_state.backfill_protect_expires_at")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }

    pub fn clear_discovery_scoring_backfill_source_protection(&self) -> Result<()> {
        self.with_immediate_transaction_retry(
            "discovery scoring backfill source protection clear",
            |conn| {
                conn.execute(
                    "DELETE FROM discovery_scoring_state
                 WHERE state_key IN ('backfill_protect_since_ts', 'backfill_protect_expires_at')",
                    [],
                )
                .context("failed clearing discovery scoring backfill source protection")?;
                Ok(0usize)
            },
        )?;
        Ok(())
    }
}
