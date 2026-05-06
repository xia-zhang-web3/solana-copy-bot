impl SqliteStore {
    fn startup_trusted_selection_gate_status_from_metadata(
        &self,
        bootstrap_required: bool,
        reason: Option<String>,
    ) -> Result<Option<StartupTrustedSelectionGateStatus>> {
        let Some(metadata) = self.latest_trusted_wallet_metrics_snapshot_metadata()? else {
            return Ok(None);
        };
        Ok(Some(StartupTrustedSelectionGateStatus {
            bootstrap_required,
            selection_state: Some(metadata.trust_state),
            startup_fail_closed: bootstrap_required
                || matches!(metadata.trust_state, TrustedSelectionState::Invalid),
            reason,
            active_snapshot_id: Some(metadata.snapshot_id),
            active_snapshot_window_start: Some(metadata.effective_window_start),
            last_bootstrap_source_kind: Some(metadata.source_kind),
            source_snapshot_window_start: metadata.source_window_start,
            legacy_bool_fallback_used: false,
        }))
    }

    pub fn discovery_trusted_selection_bootstrap_required(&self) -> Result<bool> {
        self.ensure_discovery_strategy_state_table()?;
        let required = self
            .conn
            .query_row(
                "SELECT trusted_selection_bootstrap_required
                 FROM discovery_strategy_state
                 WHERE id = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .context("failed reading discovery trusted selection bootstrap requirement")?
            .unwrap_or(0);
        Ok(required != 0)
    }

    pub fn set_discovery_trusted_selection_bootstrap_required(
        &self,
        required: bool,
        reason: &str,
    ) -> Result<()> {
        self.ensure_discovery_strategy_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_strategy_state(
                    id,
                    trusted_selection_bootstrap_required,
                    trusted_selection_reason,
                    updated_at
                 ) VALUES (1, ?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET
                    trusted_selection_bootstrap_required =
                        excluded.trusted_selection_bootstrap_required,
                    trusted_selection_reason = excluded.trusted_selection_reason,
                    updated_at = excluded.updated_at",
                params![
                    if required { 1 } else { 0 },
                    reason,
                    Utc::now().to_rfc3339()
                ],
            )
        })
        .context("failed updating discovery trusted selection bootstrap requirement")?;
        Ok(())
    }
    pub fn discovery_bootstrap_degraded_state(&self) -> Result<DiscoveryBootstrapDegradedStateRow> {
        self.ensure_discovery_strategy_state_table()?;
        discovery_bootstrap_degraded_state_query(&self.conn)
    }

    pub fn discovery_bootstrap_degraded_state_read_only(
        &self,
    ) -> Result<DiscoveryBootstrapDegradedStateRow> {
        if !self.sqlite_table_exists("discovery_strategy_state")? {
            return Ok(DiscoveryBootstrapDegradedStateRow::default());
        }
        discovery_bootstrap_degraded_state_query(&self.conn)
    }

    pub fn discovery_recent_raw_restore_state(&self) -> Result<DiscoveryRecentRawRestoreStateRow> {
        self.ensure_discovery_recent_raw_restore_state_table()?;
        discovery_recent_raw_restore_state_query(&self.conn)
    }

    pub fn discovery_recent_raw_restore_state_read_only(
        &self,
    ) -> Result<DiscoveryRecentRawRestoreStateRow> {
        if !self.sqlite_table_exists("discovery_recent_raw_restore_state")? {
            return Ok(DiscoveryRecentRawRestoreStateRow::default());
        }
        discovery_recent_raw_restore_state_query(&self.conn)
    }

    #[allow(dead_code)]
    pub(crate) fn runtime_artifact_restore_dirty_tables(
        &self,
    ) -> Result<Vec<RuntimeArtifactRestoreDirtyTable>> {
        runtime_artifact_restore_dirty_tables_on_conn(&self.conn)
    }

    pub fn set_discovery_bootstrap_degraded_state(
        &self,
        active: bool,
        reason: Option<&str>,
        armed_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        self.ensure_discovery_strategy_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_strategy_state(
                    id,
                    bootstrap_degraded_active,
                    bootstrap_degraded_reason,
                    bootstrap_degraded_armed_at,
                    updated_at
                 ) VALUES (1, ?1, ?2, ?3, ?4)
                 ON CONFLICT(id) DO UPDATE SET
                    bootstrap_degraded_active = excluded.bootstrap_degraded_active,
                    bootstrap_degraded_reason = excluded.bootstrap_degraded_reason,
                    bootstrap_degraded_armed_at = excluded.bootstrap_degraded_armed_at,
                    updated_at = excluded.updated_at",
                params![
                    if active { 1 } else { 0 },
                    reason,
                    armed_at.map(|ts| ts.to_rfc3339()),
                    Utc::now().to_rfc3339(),
                ],
            )
        })
        .context("failed updating discovery bootstrap-degraded state")?;
        Ok(())
    }

    pub fn set_discovery_recent_raw_restore_state(
        &self,
        update: &DiscoveryRecentRawRestoreStateUpdate,
    ) -> Result<()> {
        self.ensure_discovery_recent_raw_restore_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_recent_raw_restore_state(
                    id,
                    journal_available,
                    journal_replayed,
                    required_window_start,
                    journal_covered_since,
                    journal_covered_through_cursor_ts,
                    journal_covered_through_cursor_slot,
                    journal_covered_through_cursor_signature,
                    gap_fill_replayed,
                    gap_fill_covered_since,
                    gap_fill_covered_through_cursor_ts,
                    gap_fill_covered_through_cursor_slot,
                    gap_fill_covered_through_cursor_signature,
                    effective_covered_since,
                    effective_covered_through_cursor_ts,
                    effective_covered_through_cursor_slot,
                    effective_covered_through_cursor_signature,
                    artifact_runtime_cursor_ts,
                    artifact_runtime_cursor_slot,
                    artifact_runtime_cursor_signature,
                    journal_covers_artifact_cursor,
                    raw_coverage_satisfied,
                    gap_fill_replayed_rows,
                    replayed_rows,
                    reason,
                    replay_started_at,
                    replay_completed_at,
                    updated_at
                 ) VALUES (
                    1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27
                 )
                 ON CONFLICT(id) DO UPDATE SET
                    journal_available = excluded.journal_available,
                    journal_replayed = excluded.journal_replayed,
                    required_window_start = excluded.required_window_start,
                    journal_covered_since = excluded.journal_covered_since,
                    journal_covered_through_cursor_ts =
                        excluded.journal_covered_through_cursor_ts,
                    journal_covered_through_cursor_slot =
                        excluded.journal_covered_through_cursor_slot,
                    journal_covered_through_cursor_signature =
                        excluded.journal_covered_through_cursor_signature,
                    gap_fill_replayed = excluded.gap_fill_replayed,
                    gap_fill_covered_since = excluded.gap_fill_covered_since,
                    gap_fill_covered_through_cursor_ts =
                        excluded.gap_fill_covered_through_cursor_ts,
                    gap_fill_covered_through_cursor_slot =
                        excluded.gap_fill_covered_through_cursor_slot,
                    gap_fill_covered_through_cursor_signature =
                        excluded.gap_fill_covered_through_cursor_signature,
                    effective_covered_since = excluded.effective_covered_since,
                    effective_covered_through_cursor_ts =
                        excluded.effective_covered_through_cursor_ts,
                    effective_covered_through_cursor_slot =
                        excluded.effective_covered_through_cursor_slot,
                    effective_covered_through_cursor_signature =
                        excluded.effective_covered_through_cursor_signature,
                    artifact_runtime_cursor_ts = excluded.artifact_runtime_cursor_ts,
                    artifact_runtime_cursor_slot = excluded.artifact_runtime_cursor_slot,
                    artifact_runtime_cursor_signature =
                        excluded.artifact_runtime_cursor_signature,
                    journal_covers_artifact_cursor =
                        excluded.journal_covers_artifact_cursor,
                    raw_coverage_satisfied = excluded.raw_coverage_satisfied,
                    gap_fill_replayed_rows = excluded.gap_fill_replayed_rows,
                    replayed_rows = excluded.replayed_rows,
                    reason = excluded.reason,
                    replay_started_at = excluded.replay_started_at,
                    replay_completed_at = excluded.replay_completed_at,
                    updated_at = excluded.updated_at",
                params![
                    if update.journal_available { 1 } else { 0 },
                    if update.journal_replayed { 1 } else { 0 },
                    update.required_window_start.map(|ts| ts.to_rfc3339()),
                    update.journal_covered_since.map(|ts| ts.to_rfc3339()),
                    update
                        .journal_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.ts_utc.to_rfc3339()),
                    update
                        .journal_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.slot as i64),
                    update
                        .journal_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.signature.as_str()),
                    if update.gap_fill_replayed { 1 } else { 0 },
                    update.gap_fill_covered_since.map(|ts| ts.to_rfc3339()),
                    update
                        .gap_fill_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.ts_utc.to_rfc3339()),
                    update
                        .gap_fill_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.slot as i64),
                    update
                        .gap_fill_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.signature.as_str()),
                    update.effective_covered_since.map(|ts| ts.to_rfc3339()),
                    update
                        .effective_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.ts_utc.to_rfc3339()),
                    update
                        .effective_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.slot as i64),
                    update
                        .effective_covered_through_cursor
                        .as_ref()
                        .map(|cursor| cursor.signature.as_str()),
                    update
                        .artifact_runtime_cursor
                        .as_ref()
                        .map(|cursor| cursor.ts_utc.to_rfc3339()),
                    update
                        .artifact_runtime_cursor
                        .as_ref()
                        .map(|cursor| cursor.slot as i64),
                    update
                        .artifact_runtime_cursor
                        .as_ref()
                        .map(|cursor| cursor.signature.as_str()),
                    if update.journal_covers_artifact_cursor {
                        1
                    } else {
                        0
                    },
                    if update.raw_coverage_satisfied { 1 } else { 0 },
                    update.gap_fill_replayed_rows as i64,
                    update.replayed_rows as i64,
                    update.reason.as_deref(),
                    update.replay_started_at.map(|ts| ts.to_rfc3339()),
                    update.replay_completed_at.map(|ts| ts.to_rfc3339()),
                    Utc::now().to_rfc3339(),
                ],
            )
        })
        .context("failed updating discovery recent raw restore state")?;
        Ok(())
    }
}
