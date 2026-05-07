use super::*;

impl DiscoveryService {
    pub(super) fn classify_recent_raw_replacement_artifact_history_contract(
        staged_candidate_scan_error: Option<&str>,
        current_fixed_candidate_exists: bool,
        current_fixed_candidate_manifest_parseable: bool,
        previous_artifact_archive_candidate_count: usize,
        previous_artifact_archive_parseable_count: usize,
    ) -> (
        RecentRawReplacementArtifactHistoryReasonClass,
        bool,
        bool,
        String,
    ) {
        if let Some(error) = staged_candidate_scan_error {
            return (
                RecentRawReplacementArtifactHistoryReasonClass::RecentRawReplacementArtifactHistoryUnprovenDueToMissingEvidence,
                false,
                true,
                format!(
                    "recent_raw replacement artifact history is unproven because the staged snapshot directory could not be read read-only: {error}"
                ),
            );
        }

        if previous_artifact_archive_parseable_count > 0 {
            return (
                RecentRawReplacementArtifactHistoryReasonClass::RecentRawReplacementArtifactHistoryArchivedElsewhere,
                true,
                false,
                format!(
                    "recent_raw replacement artifact history is present under discoverable staged sidecar artifact paths. previous_artifact_archive_candidate_count={}, previous_artifact_archive_parseable_count={}",
                    previous_artifact_archive_candidate_count,
                    previous_artifact_archive_parseable_count,
                ),
            );
        }

        if previous_artifact_archive_candidate_count > 0 {
            return (
                RecentRawReplacementArtifactHistoryReasonClass::RecentRawReplacementArtifactHistoryUnprovenDueToMissingEvidence,
                false,
                true,
                format!(
                    "recent_raw replacement artifact history sidecar paths are present, but none are parseable enough for cross-attempt comparison. previous_artifact_archive_candidate_count={}, previous_artifact_archive_parseable_count=0",
                    previous_artifact_archive_candidate_count,
                ),
            );
        }

        if current_fixed_candidate_exists && current_fixed_candidate_manifest_parseable {
            return (
                RecentRawReplacementArtifactHistoryReasonClass::RecentRawReplacementArtifactHistoryFixedPathOverwriteByDesign,
                true,
                false,
                "recent_raw replacement artifact history is absent as expected under the current fixed staged path contract: the replacement path uses one fixed staged snapshot and metadata pair, resumes and rewrites that pair in place, and does not persist per-attempt replacement history sidecars by default".to_string(),
            );
        }

        (
            RecentRawReplacementArtifactHistoryReasonClass::RecentRawReplacementArtifactHistoryUnprovenDueToMissingEvidence,
            false,
            true,
            format!(
                "recent_raw replacement artifact history is unproven because no parseable current fixed staged replacement candidate is available. current_fixed_candidate_exists={}, current_fixed_candidate_manifest_parseable={}",
                current_fixed_candidate_exists,
                current_fixed_candidate_manifest_parseable,
            ),
        )
    }

    pub(super) fn classify_recent_raw_replacement_attempt_telemetry(
        artifact_count: usize,
        parseable_count: usize,
        probe_mode: &str,
        deep_scan_used: bool,
        explicit_path_count: usize,
        scan_truncated: bool,
        latest_state: Option<&str>,
        latest_path: Option<&str>,
        proves_advancing: bool,
        proves_stalled: bool,
        proves_reset_or_recreated: bool,
    ) -> (
        RecentRawReplacementAttemptTelemetryReasonClass,
        bool,
        String,
    ) {
        if parseable_count == 0 {
            return (
                RecentRawReplacementAttemptTelemetryReasonClass::RecentRawReplacementAttemptTelemetryMissingOrUnparseable,
                false,
                format!(
                    "recent_raw replacement attempt telemetry is missing or unparseable from on-disk artifact discovery. probe_mode={}, deep_scan_used={}, explicit_paths_checked={}, telemetry_artifact_count={}, parseable_count={}, scan_truncated={}. Default bounded mode checks exact known paths only and does not read artifact directories; without a durable discovery_recent_raw_snapshot telemetry artifact in that evidence set, fixed replacement progress cannot be proven from persisted attempt telemetry in this batch",
                    probe_mode,
                    deep_scan_used,
                    explicit_path_count,
                    artifact_count,
                    parseable_count,
                    scan_truncated,
                ),
            );
        }

        if proves_reset_or_recreated {
            return (
                RecentRawReplacementAttemptTelemetryReasonClass::RecentRawReplacementAttemptTelemetryResetOrRecreated,
                true,
                format!(
                    "latest persisted recent_raw snapshot-attempt telemetry proves reset/recreated behavior via explicit staged reseed/reset evidence. latest_path={}, latest_state={}",
                    latest_path.unwrap_or("null"),
                    latest_state.unwrap_or("null"),
                ),
            );
        }

        if proves_advancing {
            return (
                RecentRawReplacementAttemptTelemetryReasonClass::RecentRawReplacementAttemptTelemetryAdvancingButIncomplete,
                true,
                format!(
                    "latest persisted recent_raw snapshot-attempt telemetry proves the fixed staged replacement candidate advanced during a deferred attempt. latest_path={}, latest_state={}",
                    latest_path.unwrap_or("null"),
                    latest_state.unwrap_or("null"),
                ),
            );
        }

        if proves_stalled {
            return (
                RecentRawReplacementAttemptTelemetryReasonClass::RecentRawReplacementAttemptTelemetryStalled,
                true,
                format!(
                    "latest persisted recent_raw snapshot-attempt telemetry proves the fixed staged replacement candidate resumed but did not advance during the attempt. latest_path={}, latest_state={}",
                    latest_path.unwrap_or("null"),
                    latest_state.unwrap_or("null"),
                ),
            );
        }

        (
            RecentRawReplacementAttemptTelemetryReasonClass::RecentRawReplacementAttemptTelemetryMissingOrUnparseable,
            false,
            format!(
                "recent_raw replacement attempt telemetry artifacts are parseable, but the latest artifact does not prove advancing, stalled, or reset/recreated progress. probe_mode={}, deep_scan_used={}, latest_path={}, latest_state={}, telemetry_artifact_count={}, parseable_count={}",
                probe_mode,
                deep_scan_used,
                latest_path.unwrap_or("null"),
                latest_state.unwrap_or("null"),
                artifact_count,
                parseable_count,
            ),
        )
    }
}
