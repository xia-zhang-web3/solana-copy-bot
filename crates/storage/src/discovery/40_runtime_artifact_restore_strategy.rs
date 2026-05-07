use super::{
    canonical_wallet_metrics_window_start, insert_trusted_wallet_metrics_snapshot_on_conn,
};
use crate::{
    DiscoveryRuntimeArtifact, TrustedSelectionState, TrustedSnapshotSourceKind,
    TrustedWalletMetricsSnapshotWrite,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};

#[allow(dead_code)]
fn restore_runtime_artifact_publication_state_on_conn(
    conn: &Connection,
    artifact: &DiscoveryRuntimeArtifact,
    restored_at: DateTime<Utc>,
    bootstrap_degraded: bool,
) -> Result<()> {
    let published_wallet_ids_json = serde_json::to_string(
        artifact
            .publication_state
            .published_wallet_ids
            .as_ref()
            .expect("validated complete publication truth above"),
    )
    .context("failed serializing runtime artifact published wallet ids")?;
    let published_window_start = artifact
        .publication_state
        .last_published_window_start
        .expect("validated complete publication truth above");
    let active_snapshot = TrustedWalletMetricsSnapshotWrite {
        snapshot_id: format!(
            "wallet_metrics:{}:{}",
            TrustedSnapshotSourceKind::DiscoveryRefresh.as_str(),
            published_window_start.to_rfc3339()
        ),
        source_snapshot_id: None,
        source_window_start: Some(published_window_start),
        effective_window_start: published_window_start,
        created_at: artifact
            .publication_state
            .last_published_at
            .unwrap_or(artifact.publication_state.updated_at),
        source_kind: TrustedSnapshotSourceKind::DiscoveryRefresh,
        row_count: artifact.published_wallet_metrics_snapshot.len(),
        trust_state: TrustedSelectionState::TrustedCurrent,
    };
    insert_trusted_wallet_metrics_snapshot_on_conn(conn, &active_snapshot)?;
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
            bootstrap_degraded_active,
            bootstrap_degraded_reason,
            bootstrap_degraded_armed_at,
            publication_runtime_mode,
            publication_reason,
            publication_last_published_at,
            publication_last_published_window_start,
            publication_scoring_source,
            publication_wallet_ids_json,
            publication_policy_fingerprint,
            updated_at
         ) VALUES (
            ?1,
            ?2,
            ?3,
            ?4,
            ?5,
            ?6,
            ?7,
            ?8,
            ?9,
            ?10,
            ?11,
            ?12,
            ?13,
            ?14,
            ?15,
            ?16,
            ?17,
            ?18,
            ?19
         )",
        params![
            1_i64,
            0_i64,
            "runtime_artifact_restore",
            TrustedSelectionState::TrustedCurrent.as_str(),
            active_snapshot.snapshot_id.as_str(),
            canonical_wallet_metrics_window_start(active_snapshot.effective_window_start),
            TrustedSnapshotSourceKind::DiscoveryRefresh.as_str(),
            restored_at.to_rfc3339(),
            if bootstrap_degraded { 1 } else { 0 },
            bootstrap_degraded.then_some("runtime_artifact_restore_bootstrap_degraded"),
            bootstrap_degraded.then_some(restored_at.to_rfc3339()),
            artifact.publication_state.runtime_mode.as_str(),
            &artifact.publication_state.reason,
            artifact
                .publication_state
                .last_published_at
                .map(|ts| ts.to_rfc3339()),
            artifact
                .publication_state
                .last_published_window_start
                .map(canonical_wallet_metrics_window_start),
            artifact
                .publication_state
                .published_scoring_source
                .as_deref(),
            published_wallet_ids_json.as_str(),
            artifact
                .publication_state
                .publication_policy_fingerprint
                .as_deref(),
            artifact.publication_state.updated_at.to_rfc3339(),
        ],
    )
    .context("failed restoring discovery publication state from artifact")?;
    Ok(())
}

#[allow(dead_code)]
fn initialize_recent_raw_restore_state_from_artifact_on_conn(
    conn: &Connection,
    artifact: &DiscoveryRuntimeArtifact,
    restored_at: DateTime<Utc>,
) -> Result<()> {
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
            1, 0, 0, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?1, ?2, ?3, 0, 0, 0, 0, ?4, NULL, NULL, ?5
         )",
        params![
            artifact.runtime_cursor.ts_utc.to_rfc3339(),
            artifact.runtime_cursor.slot as i64,
            artifact.runtime_cursor.signature.as_str(),
            "journal_replay_pending",
            restored_at.to_rfc3339(),
        ],
    )
    .context("failed initializing discovery recent raw restore state from artifact")?;
    Ok(())
}
