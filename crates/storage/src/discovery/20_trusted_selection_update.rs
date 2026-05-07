use super::canonical_wallet_metrics_window_start;
use crate::{DiscoveryTrustedSelectionStateUpdate, SqliteStore, TrustedSnapshotSourceKind};
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::params;

impl SqliteStore {
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
                        .map(canonical_wallet_metrics_window_start),
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
}
