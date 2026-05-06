use crate::observed::parse_rfc3339_utc;
use crate::{
    DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateRow, DiscoveryRuntimeArtifact,
    DiscoveryRuntimeCursor, DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::OptionalExtension;
use std::collections::HashSet;

pub(super) fn load_discovery_runtime_cursor_on_conn(
    conn: &rusqlite::Connection,
) -> Result<Option<DiscoveryRuntimeCursor>> {
    let runtime_cursor: Option<(String, i64, String)> = conn
        .query_row(
            "SELECT cursor_ts, cursor_slot, cursor_signature
             FROM discovery_runtime_state
             WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .context("failed reading discovery runtime cursor for artifact export")?;
    runtime_cursor
        .map(|(ts_raw, slot_raw, signature)| {
            if slot_raw < 0 {
                return Err(anyhow::anyhow!(
                    "invalid negative discovery_runtime_state.cursor_slot: {slot_raw}"
                ));
            }
            Ok(DiscoveryRuntimeCursor {
                ts_utc: parse_rfc3339_utc(&ts_raw, "discovery_runtime_state.cursor_ts")?,
                slot: slot_raw as u64,
                signature,
            })
        })
        .transpose()
}

pub(super) fn validate_runtime_artifact_snapshot_shape(
    artifact: &DiscoveryRuntimeArtifact,
) -> Result<()> {
    if artifact.format_version != DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION {
        return Err(anyhow::anyhow!(
            "unsupported discovery runtime artifact format_version={} expected={}",
            artifact.format_version,
            DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION
        ));
    }
    if !artifact.publication_state.has_complete_publication_truth() {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact publication truth is incomplete"
        ));
    }
    let published_window_start = artifact
        .publication_state
        .last_published_window_start
        .expect("validated complete publication truth above");
    if artifact.published_wallet_metrics_snapshot.is_empty() {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact is missing published wallet_metrics snapshot rows"
        ));
    }
    if artifact
        .published_wallet_metrics_snapshot
        .iter()
        .any(|row| row.window_start != published_window_start)
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact wallet_metrics snapshot rows do not match publication window_start={}",
            published_window_start.to_rfc3339()
        ));
    }
    let snapshot_wallet_ids: Vec<String> = artifact
        .published_wallet_metrics_snapshot
        .iter()
        .map(|row| row.wallet_id.clone())
        .collect();
    let snapshot_unique_wallet_ids: HashSet<String> = snapshot_wallet_ids.iter().cloned().collect();
    if snapshot_unique_wallet_ids.len() != snapshot_wallet_ids.len() {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact wallet_metrics snapshot contains duplicate wallet rows"
        ));
    }
    let published_wallet_ids = artifact
        .publication_state
        .published_wallet_ids
        .as_ref()
        .expect("validated complete publication truth above");
    let published_unique_wallet_ids: HashSet<String> =
        published_wallet_ids.iter().cloned().collect();
    if published_unique_wallet_ids.len() != published_wallet_ids.len() {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact publication truth contains duplicate published wallet ids"
        ));
    }
    if snapshot_wallet_ids.len() != published_wallet_ids.len()
        || snapshot_unique_wallet_ids != published_unique_wallet_ids
    {
        return Err(anyhow::anyhow!(
            "discovery runtime artifact wallet_metrics snapshot must exactly match published wallet ids"
        ));
    }
    Ok(())
}

pub(super) fn runtime_artifact_export_truth_detail(
    publication_state: &DiscoveryPublicationStateRow,
    gate: &DiscoveryPublicationFreshnessGate,
    now: DateTime<Utc>,
) -> String {
    let missing_fields = incomplete_publication_truth_fields(publication_state);
    format!(
        "runtime_mode={} reason={} complete={} fresh_under_export_gate={} identity_matches={} published_scoring_source={} expected_scoring_source={} policy_fingerprint={} expected_policy_fingerprint={} last_published_at={} last_published_window_start={} published_wallet_count={} updated_at={} gate_scoring_window_days={} gate_metric_snapshot_interval_seconds={} gate_refresh_seconds={} missing_fields={}",
        publication_state.runtime_mode.as_str(),
        publication_state.reason,
        publication_state.has_complete_publication_truth(),
        publication_state.is_fresh_under_gate(gate, now),
        publication_state.matches_expected_publication_identity(gate),
        publication_state
            .published_scoring_source
            .as_deref()
            .unwrap_or("null"),
        gate.expected_scoring_source.as_deref().unwrap_or("null"),
        publication_state
            .publication_policy_fingerprint
            .as_deref()
            .unwrap_or("null"),
        gate.expected_policy_fingerprint.as_deref().unwrap_or("null"),
        format_optional_export_ts(publication_state.last_published_at),
        format_optional_export_ts(publication_state.last_published_window_start),
        publication_state
            .published_wallet_ids
            .as_ref()
            .map(Vec::len)
            .unwrap_or(0),
        publication_state.updated_at.to_rfc3339(),
        gate.scoring_window_days,
        gate.metric_snapshot_interval_seconds,
        gate.refresh_seconds,
        if missing_fields.is_empty() {
            "none".to_string()
        } else {
            missing_fields.join(",")
        }
    )
}

fn incomplete_publication_truth_fields(
    publication_state: &DiscoveryPublicationStateRow,
) -> Vec<&'static str> {
    let mut missing_fields = Vec::new();
    if publication_state.last_published_at.is_none() {
        missing_fields.push("last_published_at");
    }
    if publication_state.last_published_window_start.is_none() {
        missing_fields.push("last_published_window_start");
    }
    if !publication_state
        .published_wallet_ids
        .as_ref()
        .is_some_and(|wallet_ids| !wallet_ids.is_empty())
    {
        missing_fields.push("published_wallet_ids");
    }
    missing_fields
}

fn format_optional_export_ts(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| "null".to_string())
}
