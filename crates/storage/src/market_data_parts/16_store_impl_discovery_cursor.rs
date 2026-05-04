impl SqliteStore {
    pub fn load_discovery_runtime_cursor(&self) -> Result<Option<DiscoveryRuntimeCursor>> {
        self.ensure_discovery_runtime_state_table()?;
        self.load_discovery_runtime_cursor_query()
    }

    pub fn load_discovery_runtime_cursor_read_only(
        &self,
    ) -> Result<Option<DiscoveryRuntimeCursor>> {
        if !self.sqlite_table_exists("discovery_runtime_state")? {
            return Ok(None);
        }
        self.load_discovery_runtime_cursor_query()
    }

    fn load_discovery_runtime_cursor_query(&self) -> Result<Option<DiscoveryRuntimeCursor>> {
        let row: Option<(String, i64, String)> = self
            .conn
            .query_row(
                "SELECT cursor_ts, cursor_slot, cursor_signature
                     FROM discovery_runtime_state
                     WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .context("failed reading discovery runtime cursor")?;
        let Some((cursor_ts_raw, cursor_slot_raw, cursor_signature)) = row else {
            return Ok(None);
        };
        let cursor_ts = DateTime::parse_from_rfc3339(cursor_ts_raw.as_str())
            .map(|dt| dt.with_timezone(&Utc))
            .with_context(|| format!("invalid discovery cursor timestamp: {cursor_ts_raw}"))?;
        Ok(Some(DiscoveryRuntimeCursor {
            ts_utc: cursor_ts,
            slot: cursor_slot_raw.max(0) as u64,
            signature: cursor_signature,
        }))
    }

    pub fn upsert_discovery_runtime_cursor(&self, cursor: &DiscoveryRuntimeCursor) -> Result<()> {
        self.ensure_discovery_runtime_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_runtime_state(
                        id, cursor_ts, cursor_slot, cursor_signature, updated_at
                     ) VALUES (1, ?1, ?2, ?3, datetime('now'))
                     ON CONFLICT(id) DO UPDATE SET
                        cursor_ts = excluded.cursor_ts,
                        cursor_slot = excluded.cursor_slot,
                        cursor_signature = excluded.cursor_signature,
                        updated_at = excluded.updated_at",
                params![
                    cursor.ts_utc.to_rfc3339(),
                    cursor.slot as i64,
                    cursor.signature.as_str(),
                ],
            )
        })
        .context("failed updating discovery runtime cursor")?;
        Ok(())
    }

    pub fn load_discovery_persisted_rebuild_state(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateRow>> {
        self.ensure_discovery_persisted_rebuild_state_table()?;
        self.load_discovery_persisted_rebuild_state_query()
    }

    pub fn load_discovery_persisted_rebuild_state_read_only(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateRow>> {
        if !self.sqlite_table_exists("discovery_persisted_rebuild_state")? {
            return Ok(None);
        }
        self.load_discovery_persisted_rebuild_state_query()
    }

    pub fn discovery_persisted_rebuild_state_table_exists_read_only(&self) -> Result<bool> {
        self.sqlite_table_exists("discovery_persisted_rebuild_state")
    }
}
