impl SqliteStore {
    pub fn load_discovery_persisted_rebuild_state_meta_lite_raw_after_table_exists_read_only(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateMetaLiteRawRow>> {
        let raw = self
            .conn
            .query_row(
                "SELECT
                    phase,
                    updated_at
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .context("failed reading discovery persisted rebuild state lite metadata")?;

        Ok(raw.map(
            |(phase_raw, updated_at_raw)| DiscoveryPersistedRebuildStateMetaLiteRawRow {
                phase_raw,
                updated_at_raw,
            },
        ))
    }

    pub fn explain_discovery_persisted_rebuild_state_meta_query_plan_after_table_exists_read_only(
        &self,
    ) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT phase, updated_at
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
            )
            .context("failed preparing discovery persisted rebuild state meta query plan")?;
        let plan_rows = stmt
            .query_map([], |row| row.get::<_, String>(3))
            .context("failed querying discovery persisted rebuild state meta query plan")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed collecting discovery persisted rebuild state meta query plan")?;
        Ok(plan_rows)
    }

    pub fn load_discovery_persisted_rebuild_state_meta_lite_raw_read_only(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateMetaLiteRawRow>> {
        if !self.sqlite_table_exists("discovery_persisted_rebuild_state")? {
            return Ok(None);
        }
        self.load_discovery_persisted_rebuild_state_meta_lite_raw_after_table_exists_read_only()
    }

    pub fn load_discovery_persisted_rebuild_state_json_bytes_after_table_exists_read_only(
        &self,
    ) -> Result<Option<usize>> {
        let state_json_bytes = self
            .conn
            .query_row(
                "SELECT length(CAST(state_json AS BLOB))
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .context("failed reading discovery persisted rebuild state json byte length")?;
        Ok(state_json_bytes.map(|bytes| bytes.max(0) as usize))
    }

    pub fn explain_discovery_persisted_rebuild_state_size_query_plan_after_table_exists_read_only(
        &self,
    ) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT length(CAST(state_json AS BLOB))
                 FROM discovery_persisted_rebuild_state
                 WHERE id = 1",
            )
            .context("failed preparing discovery persisted rebuild state size query plan")?;
        let plan_rows = stmt
            .query_map([], |row| row.get::<_, String>(3))
            .context("failed querying discovery persisted rebuild state size query plan")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed collecting discovery persisted rebuild state size query plan")?;
        Ok(plan_rows)
    }

    pub fn load_discovery_persisted_rebuild_state_json_bytes_read_only(
        &self,
    ) -> Result<Option<usize>> {
        if !self.sqlite_table_exists("discovery_persisted_rebuild_state")? {
            return Ok(None);
        }
        self.load_discovery_persisted_rebuild_state_json_bytes_after_table_exists_read_only()
    }

    pub fn load_discovery_persisted_rebuild_state_meta_read_only(
        &self,
    ) -> Result<Option<DiscoveryPersistedRebuildStateMetaRow>> {
        let Some(raw) = self.load_discovery_persisted_rebuild_state_meta_lite_raw_read_only()?
        else {
            return Ok(None);
        };
        Ok(Some(DiscoveryPersistedRebuildStateMetaRow {
            phase: DiscoveryPersistedRebuildPhase::parse(&raw.phase_raw)?,
            state_json_bytes: self
                .load_discovery_persisted_rebuild_state_json_bytes_read_only()?
                .unwrap_or(0),
            updated_at: parse_rfc3339_utc(
                &raw.updated_at_raw,
                "discovery_persisted_rebuild_state.updated_at",
            )?,
        }))
    }
}
