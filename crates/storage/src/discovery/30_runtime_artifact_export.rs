use super::{
    parse_rfc3339_utc, runtime_artifact_export_truth_detail,
    validate_runtime_artifact_snapshot_shape,
};
use crate::{
    DiscoveryPublicationFreshnessGate, DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor,
    DiscoveryRuntimeMode, SqliteStore, DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::OptionalExtension;
use std::collections::HashSet;

impl SqliteStore {
    pub fn export_discovery_runtime_artifact(
        &self,
        exported_at: DateTime<Utc>,
        export_gate: DiscoveryPublicationFreshnessGate,
    ) -> Result<DiscoveryRuntimeArtifact> {
        self.with_deferred_transaction("discovery runtime artifact export", |_conn| {
            let publication_state = self
                .discovery_publication_state_read_only()?
                .ok_or_else(|| anyhow::anyhow!("discovery runtime artifact export requires persisted publication truth"))?;
            let truth_detail =
                runtime_artifact_export_truth_detail(&publication_state, &export_gate, exported_at);
            if export_gate.expected_scoring_source.is_none()
                || export_gate.expected_policy_fingerprint.is_none()
            {
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact export requires explicit expected discovery v2 export gate identity ({truth_detail})"
                ));
            }
            if publication_state.runtime_mode != DiscoveryRuntimeMode::Healthy {
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact export requires healthy publication state ({truth_detail})"
                ));
            }
            if !publication_state.has_complete_publication_truth() {
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact export requires complete publication truth ({truth_detail})"
                ));
            }
            if publication_state.published_scoring_source.as_deref()
                != Some("discovery_v2_operational_window")
                || publication_state.publication_policy_fingerprint.is_none()
                || publication_state.publication_runtime_cursor.is_none()
            {
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact export requires complete discovery v2 publication identity ({truth_detail})"
                ));
            }
            if !publication_state.matches_expected_publication_identity(&export_gate) {
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact export requires expected discovery v2 publication identity ({truth_detail})"
                ));
            }
            if !publication_state.is_fresh_under_gate(&export_gate, exported_at) {
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact export requires fresh publication truth under export gate ({truth_detail})"
                ));
            }
            let runtime_cursor: Option<(String, i64, String)> = self
                .conn
                .query_row(
                    "SELECT cursor_ts, cursor_slot, cursor_signature
                     FROM discovery_runtime_state
                     WHERE id = 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .context("failed reading discovery runtime cursor for artifact export")?;
            let Some((cursor_ts_raw, cursor_slot_raw, cursor_signature)) = runtime_cursor else {
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact export requires a persisted discovery runtime cursor ({truth_detail})"
                ));
            };
            if cursor_slot_raw < 0 {
                return Err(anyhow::anyhow!(
                    "invalid negative discovery_runtime_state.cursor_slot: {cursor_slot_raw}"
                ));
            }
            let runtime_cursor = DiscoveryRuntimeCursor {
                ts_utc: parse_rfc3339_utc(
                    &cursor_ts_raw,
                    "discovery_runtime_state.cursor_ts",
                )?,
                slot: cursor_slot_raw as u64,
                signature: cursor_signature,
            };
            if publication_state.publication_runtime_cursor.as_ref() != Some(&runtime_cursor) {
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact export requires publication-bound runtime cursor ({truth_detail})"
                ));
            }
            if !publication_state
                .has_fresh_publication_runtime_cursor_under_gate(&export_gate, exported_at)
            {
                return Err(anyhow::anyhow!(
                    "discovery runtime artifact export requires fresh publication-bound runtime cursor ({truth_detail})"
                ));
            }
            let published_window_start = publication_state
                .last_published_window_start
                .expect("validated complete publication truth above");
            let published_wallet_ids = publication_state
                .published_wallet_ids
                .as_ref()
                .expect("validated complete publication truth above");
            let published_wallet_ids = published_wallet_ids
                .iter()
                .cloned()
                .collect::<HashSet<_>>();
            let published_wallet_metrics_snapshot = self
                .load_wallet_metric_snapshots_for_window(published_window_start)?
                .into_iter()
                .filter(|row| published_wallet_ids.contains(&row.wallet_id))
                .collect();
            let artifact = DiscoveryRuntimeArtifact {
                format_version: DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
                exported_at,
                export_gate,
                publication_state,
                runtime_cursor,
                published_wallet_metrics_snapshot,
            };
            validate_runtime_artifact_snapshot_shape(&artifact)?;
            Ok(artifact)
        })
    }
}
