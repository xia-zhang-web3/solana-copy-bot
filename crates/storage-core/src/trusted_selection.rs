use crate::{
    DiscoveryTrustedSelectionStateRow, DiscoveryTrustedSelectionStateUpdate, SqliteDiscoveryStore,
    StartupTrustedSelectionGateStatus, TrustedSelectionState, TrustedSnapshotSourceKind,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    fn ensure_discovery_strategy_state_table(&self) -> Result<()> {
        crate::schema::ensure_discovery_strategy_state_table(self)
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

    pub fn set_discovery_trusted_selection_state(
        &self,
        update: &DiscoveryTrustedSelectionStateUpdate,
    ) -> Result<()> {
        self.ensure_discovery_strategy_state_table()?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO discovery_strategy_state(
                    id,
                    trusted_selection_bootstrap_required,
                    trusted_selection_reason,
                    trusted_selection_state,
                    active_trusted_snapshot_id,
                    active_trusted_snapshot_window_start,
                    last_trusted_bootstrap_source_kind,
                    last_trusted_bootstrap_at,
                    updated_at
                 ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(id) DO UPDATE SET
                    trusted_selection_bootstrap_required =
                        excluded.trusted_selection_bootstrap_required,
                    trusted_selection_reason = excluded.trusted_selection_reason,
                    trusted_selection_state = excluded.trusted_selection_state,
                    active_trusted_snapshot_id = excluded.active_trusted_snapshot_id,
                    active_trusted_snapshot_window_start =
                        excluded.active_trusted_snapshot_window_start,
                    last_trusted_bootstrap_source_kind =
                        excluded.last_trusted_bootstrap_source_kind,
                    last_trusted_bootstrap_at = excluded.last_trusted_bootstrap_at,
                    updated_at = excluded.updated_at",
                params![
                    if update.bootstrap_required { 1 } else { 0 },
                    &update.reason,
                    update.selection_state.as_str(),
                    &update.active_snapshot_id,
                    update
                        .active_snapshot_window_start
                        .map(|ts| ts.to_rfc3339()),
                    update
                        .last_bootstrap_source_kind
                        .map(TrustedSnapshotSourceKind::as_str),
                    update.last_bootstrap_at.map(|ts| ts.to_rfc3339()),
                    Utc::now().to_rfc3339(),
                ],
            )
        })
        .context("failed updating discovery trusted selection state")?;
        Ok(())
    }

    pub fn discovery_trusted_selection_state(
        &self,
    ) -> Result<Option<DiscoveryTrustedSelectionStateRow>> {
        self.ensure_discovery_strategy_state_table()?;
        let raw = self
            .conn
            .query_row(
                "SELECT
                    trusted_selection_bootstrap_required,
                    trusted_selection_reason,
                    trusted_selection_state,
                    active_trusted_snapshot_id,
                    active_trusted_snapshot_window_start,
                    last_trusted_bootstrap_source_kind,
                    last_trusted_bootstrap_at,
                    updated_at
                 FROM discovery_strategy_state
                 WHERE id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .optional()
            .context("failed reading discovery trusted selection state")?;
        raw.map(
            |(
                bootstrap_required,
                reason,
                selection_state_raw,
                active_snapshot_id,
                active_snapshot_window_start_raw,
                last_bootstrap_source_kind_raw,
                last_bootstrap_at_raw,
                updated_at_raw,
            )| {
                Ok(DiscoveryTrustedSelectionStateRow {
                    bootstrap_required: bootstrap_required != 0,
                    reason,
                    selection_state: TrustedSelectionState::parse(&selection_state_raw)?,
                    active_snapshot_id,
                    active_snapshot_window_start: parse_optional_rfc3339_utc(
                        active_snapshot_window_start_raw,
                    )?,
                    last_bootstrap_source_kind: match last_bootstrap_source_kind_raw {
                        Some(raw) => Some(TrustedSnapshotSourceKind::parse(&raw)?),
                        None => None,
                    },
                    last_bootstrap_at: parse_optional_rfc3339_utc(last_bootstrap_at_raw)?,
                    updated_at: parse_rfc3339_utc(&updated_at_raw)?,
                })
            },
        )
        .transpose()
    }

    pub fn startup_trusted_selection_gate_status(
        &self,
    ) -> Result<StartupTrustedSelectionGateStatus> {
        let typed_state = self.discovery_trusted_selection_state()?;
        if let Some(typed_state) = typed_state {
            let legacy_bool_only_row = typed_state.selection_state
                == TrustedSelectionState::Invalid
                && typed_state.active_snapshot_id.is_none()
                && typed_state.active_snapshot_window_start.is_none()
                && typed_state.last_bootstrap_source_kind.is_none()
                && typed_state.last_bootstrap_at.is_none();
            if legacy_bool_only_row {
                return Ok(StartupTrustedSelectionGateStatus {
                    bootstrap_required: typed_state.bootstrap_required,
                    selection_state: None,
                    startup_fail_closed: typed_state.bootstrap_required,
                    reason: Some(typed_state.reason),
                    active_snapshot_id: None,
                    active_snapshot_window_start: None,
                    last_bootstrap_source_kind: None,
                    source_snapshot_window_start: None,
                    legacy_bool_fallback_used: true,
                });
            }
            return Ok(StartupTrustedSelectionGateStatus {
                bootstrap_required: typed_state.bootstrap_required,
                selection_state: Some(typed_state.selection_state),
                startup_fail_closed: typed_state.bootstrap_required
                    || matches!(typed_state.selection_state, TrustedSelectionState::Invalid),
                reason: Some(typed_state.reason),
                active_snapshot_id: typed_state.active_snapshot_id,
                active_snapshot_window_start: typed_state.active_snapshot_window_start,
                last_bootstrap_source_kind: typed_state.last_bootstrap_source_kind,
                source_snapshot_window_start: None,
                legacy_bool_fallback_used: false,
            });
        }
        let bootstrap_required = self.discovery_trusted_selection_bootstrap_required()?;
        Ok(StartupTrustedSelectionGateStatus {
            bootstrap_required,
            selection_state: None,
            startup_fail_closed: bootstrap_required,
            reason: None,
            active_snapshot_id: None,
            active_snapshot_window_start: None,
            last_bootstrap_source_kind: None,
            source_snapshot_window_start: None,
            legacy_bool_fallback_used: true,
        })
    }
}

fn parse_optional_rfc3339_utc(raw: Option<String>) -> Result<Option<DateTime<Utc>>> {
    raw.map(|value| parse_rfc3339_utc(&value)).transpose()
}

fn parse_rfc3339_utc(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid rfc3339 timestamp: {raw}"))
}
