use super::types::ExportOutput;
use chrono::{DateTime, Utc};
use copybot_storage_core::DiscoveryRuntimeArtifact;
use std::path::{Path, PathBuf};

pub(super) fn render_output(
    state: &str,
    config_path: &Path,
    db_path: &Path,
    output_path: &Path,
    archive_path: Option<&Path>,
    cadence_minutes: Option<u64>,
    retention: Option<usize>,
    pruned_archive_paths: &[PathBuf],
    artifact: &DiscoveryRuntimeArtifact,
    fresh_under_export_gate: bool,
    fresh_under_current_gate: bool,
) -> ExportOutput {
    ExportOutput {
        event: "discovery_runtime_export".to_string(),
        state: state.to_string(),
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        output_path: output_path.display().to_string(),
        archive_path: archive_path.map(|path| path.display().to_string()),
        cadence_minutes,
        retention,
        pruned_archive_paths: pruned_archive_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        exported_at: artifact.exported_at,
        publication_runtime_mode: artifact.publication_state.runtime_mode.as_str().to_string(),
        publication_reason: artifact.publication_state.reason.clone(),
        publication_truth_complete: artifact.publication_state.has_complete_publication_truth(),
        publication_identity_matches: artifact
            .publication_state
            .matches_expected_publication_identity(&artifact.export_gate),
        published_scoring_source: artifact.publication_state.published_scoring_source.clone(),
        expected_scoring_source: artifact.export_gate.expected_scoring_source.clone(),
        publication_policy_fingerprint: artifact
            .publication_state
            .publication_policy_fingerprint
            .clone(),
        expected_policy_fingerprint: artifact.export_gate.expected_policy_fingerprint.clone(),
        fresh_under_export_gate,
        last_published_at: artifact.publication_state.last_published_at,
        last_published_window_start: artifact.publication_state.last_published_window_start,
        published_wallet_count: artifact
            .publication_state
            .published_wallet_ids
            .as_ref()
            .map(Vec::len)
            .unwrap_or(0),
        wallet_metrics_snapshot_rows: artifact.published_wallet_metrics_snapshot.len(),
        fresh_under_current_gate,
        runtime_cursor_ts: artifact.runtime_cursor.ts_utc,
        runtime_cursor_slot: artifact.runtime_cursor.slot,
        runtime_cursor_signature: artifact.runtime_cursor.signature.clone(),
    }
}

pub(super) fn render_human(output: &ExportOutput) -> String {
    [
        format!("event={}", output.event),
        format!("state={}", output.state),
        format!("config_path={}", output.config_path),
        format!("db_path={}", output.db_path),
        format!("output_path={}", output.output_path),
        format!(
            "archive_path={}",
            output.archive_path.as_deref().unwrap_or("null")
        ),
        format!(
            "cadence_minutes={}",
            output
                .cadence_minutes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "retention={}",
            output
                .retention
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("pruned_archives={}", output.pruned_archive_paths.len()),
        format!("exported_at={}", output.exported_at.to_rfc3339()),
        format!(
            "publication_runtime_mode={}",
            output.publication_runtime_mode
        ),
        format!("publication_reason={}", output.publication_reason),
        format!(
            "publication_truth_complete={}",
            output.publication_truth_complete
        ),
        format!(
            "publication_identity_matches={}",
            output.publication_identity_matches
        ),
        format!(
            "published_scoring_source={}",
            output.published_scoring_source.as_deref().unwrap_or("null")
        ),
        format!(
            "expected_scoring_source={}",
            output.expected_scoring_source.as_deref().unwrap_or("null")
        ),
        format!(
            "publication_policy_fingerprint={}",
            output
                .publication_policy_fingerprint
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "expected_policy_fingerprint={}",
            output
                .expected_policy_fingerprint
                .as_deref()
                .unwrap_or("null")
        ),
        format!("fresh_under_export_gate={}", output.fresh_under_export_gate),
        format!(
            "last_published_at={}",
            format_optional_ts(output.last_published_at.as_ref())
        ),
        format!(
            "last_published_window_start={}",
            format_optional_ts(output.last_published_window_start.as_ref())
        ),
        format!("published_wallet_count={}", output.published_wallet_count),
        format!(
            "wallet_metrics_snapshot_rows={}",
            output.wallet_metrics_snapshot_rows
        ),
        format!(
            "fresh_under_current_gate={}",
            output.fresh_under_current_gate
        ),
        format!(
            "runtime_cursor_ts={}",
            output.runtime_cursor_ts.to_rfc3339()
        ),
        format!("runtime_cursor_slot={}", output.runtime_cursor_slot),
        format!(
            "runtime_cursor_signature={}",
            output.runtime_cursor_signature
        ),
    ]
    .join("\n")
}

fn format_optional_ts(value: Option<&DateTime<Utc>>) -> String {
    value
        .map(DateTime::to_rfc3339)
        .unwrap_or_else(|| "null".to_string())
}
