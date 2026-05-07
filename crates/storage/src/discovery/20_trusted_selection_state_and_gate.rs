use super::{parse_optional_rfc3339_utc, parse_rfc3339_utc};
use crate::{
    DiscoveryTrustedSelectionStateRow, SqliteStore, StartupTrustedSelectionGateStatus,
    TrustedSelectionState, TrustedSnapshotSourceKind,
};
use anyhow::{Context, Result};
use rusqlite::OptionalExtension;

impl SqliteStore {
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
                        "discovery_strategy_state.active_trusted_snapshot_window_start",
                    )?,
                    last_bootstrap_source_kind: match last_bootstrap_source_kind_raw {
                        Some(raw) => Some(TrustedSnapshotSourceKind::parse(&raw)?),
                        None => None,
                    },
                    last_bootstrap_at: parse_optional_rfc3339_utc(
                        last_bootstrap_at_raw,
                        "discovery_strategy_state.last_trusted_bootstrap_at",
                    )?,
                    updated_at: parse_rfc3339_utc(
                        &updated_at_raw,
                        "discovery_strategy_state.updated_at",
                    )?,
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
                if let Some(status) = self.startup_trusted_selection_gate_status_from_metadata(
                    typed_state.bootstrap_required,
                    Some(typed_state.reason.clone()),
                )? {
                    return Ok(status);
                }
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
            let source_snapshot_window_start = typed_state
                .active_snapshot_window_start
                .map(|window_start| {
                    self.trusted_wallet_metrics_snapshot_metadata_for_window(window_start)
                })
                .transpose()?
                .flatten()
                .and_then(|metadata| metadata.source_window_start);
            return Ok(StartupTrustedSelectionGateStatus {
                bootstrap_required: typed_state.bootstrap_required,
                selection_state: Some(typed_state.selection_state),
                startup_fail_closed: typed_state.bootstrap_required
                    || matches!(typed_state.selection_state, TrustedSelectionState::Invalid),
                reason: Some(typed_state.reason),
                active_snapshot_id: typed_state.active_snapshot_id,
                active_snapshot_window_start: typed_state.active_snapshot_window_start,
                last_bootstrap_source_kind: typed_state.last_bootstrap_source_kind,
                source_snapshot_window_start,
                legacy_bool_fallback_used: false,
            });
        }

        let bootstrap_required = self.discovery_trusted_selection_bootstrap_required()?;
        if let Some(status) =
            self.startup_trusted_selection_gate_status_from_metadata(bootstrap_required, None)?
        {
            return Ok(status);
        }
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
