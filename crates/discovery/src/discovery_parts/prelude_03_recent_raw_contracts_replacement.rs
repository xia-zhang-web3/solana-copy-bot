#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawReplacementPromotionReasonClass {
    RecentRawReplacementCandidateMissing,
    RecentRawReplacementCandidateIncompleteAgainstCurrentSource,
    RecentRawReplacementCandidateNotNewerThanPromoted,
    RecentRawReplacementCandidateReadyToPromote,
    RecentRawReplacementPromotionContractUnprovenDueToMissingEvidence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecentRawReplacementPromotionContractDiagnostic {
    pub recent_raw_snapshot_dir: String,
    pub recent_raw_replacement_promotion_observed: bool,
    pub recent_raw_replacement_promotion_reason_class: RecentRawReplacementPromotionReasonClass,
    pub recent_raw_replacement_promotion_explanation: String,
    pub recent_raw_replacement_candidate_exists: bool,
    pub recent_raw_replacement_candidate_source_db_matches_promoted: Option<bool>,
    pub recent_raw_replacement_candidate_start_matches_current_source: Option<bool>,
    pub recent_raw_replacement_candidate_covered_through_relation_to_promoted:
        RecentRawLineageRelation,
    pub recent_raw_replacement_candidate_row_count_relation_to_promoted: RecentRawLineageRelation,
    pub recent_raw_replacement_candidate_complete_against_current_source: Option<bool>,
    pub recent_raw_replacement_candidate_promotable_now: Option<bool>,
    pub recent_raw_replacement_promotion_would_retire_current_promoted_truth: Option<bool>,
    pub recent_raw_stage3_blocked_on_replacement_candidate: Option<bool>,
    pub recent_raw_promotion_reason_class: RecentRawPromotionBlockerReasonClass,
    pub recent_raw_promotion_ready_now: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawReplacementProgressReasonClass {
    RecentRawReplacementProgressAdvancingButIncomplete,
    RecentRawReplacementProgressStalledOnSameCandidate,
    RecentRawReplacementProgressResetOrRecreated,
    RecentRawReplacementProgressUnprovenDueToMissingEvidence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecentRawReplacementProgressContractDiagnostic {
    pub recent_raw_snapshot_dir: String,
    pub recent_raw_replacement_progress_observed: bool,
    pub recent_raw_replacement_progress_reason_class: RecentRawReplacementProgressReasonClass,
    pub recent_raw_replacement_progress_explanation: String,
    pub recent_raw_replacement_candidate_exists: bool,
    pub recent_raw_replacement_candidate_created_at: Option<DateTime<Utc>>,
    pub recent_raw_replacement_candidate_row_count: Option<usize>,
    pub recent_raw_replacement_candidate_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_replacement_candidate_last_batch_completed_at: Option<DateTime<Utc>>,
    pub recent_raw_replacement_candidate_completed_after_creation: Option<bool>,
    pub recent_raw_replacement_candidate_same_identity_as_previous_attempt: Option<bool>,
    pub recent_raw_replacement_candidate_row_count_advanced: Option<bool>,
    pub recent_raw_replacement_candidate_covered_through_advanced: Option<bool>,
    pub recent_raw_replacement_candidate_last_batch_completed_at_advanced: Option<bool>,
    pub recent_raw_replacement_candidate_advancing: Option<bool>,
    pub recent_raw_replacement_candidate_stalled: Option<bool>,
    pub recent_raw_replacement_candidate_reset_or_recreated: Option<bool>,
    pub recent_raw_replacement_previous_candidate_exists: bool,
    pub recent_raw_replacement_previous_candidate_snapshot_path: Option<String>,
    pub recent_raw_replacement_previous_candidate_metadata_path: Option<String>,
    pub recent_raw_replacement_previous_candidate_created_at: Option<DateTime<Utc>>,
    pub recent_raw_replacement_previous_candidate_row_count: Option<usize>,
    pub recent_raw_replacement_previous_candidate_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_replacement_previous_candidate_last_batch_completed_at: Option<DateTime<Utc>>,
    pub recent_raw_replacement_manifest_matches_sqlite_content: Option<bool>,
    pub recent_raw_replacement_manifest_sqlite_match_unproven: bool,
    pub recent_raw_replacement_sqlite_inspection_error: Option<String>,
    pub recent_raw_replacement_candidate_complete_against_current_source: Option<bool>,
    pub recent_raw_replacement_promotion_reason_class: RecentRawReplacementPromotionReasonClass,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawReplacementArtifactHistoryReasonClass {
    RecentRawReplacementArtifactHistoryFixedPathOverwriteByDesign,
    RecentRawReplacementArtifactHistoryArchivedElsewhere,
    RecentRawReplacementArtifactHistoryUnprovenDueToMissingEvidence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecentRawReplacementArtifactHistoryContractDiagnostic {
    pub recent_raw_snapshot_dir: String,
    pub recent_raw_replacement_artifact_history_observed: bool,
    pub recent_raw_replacement_artifact_history_reason_class:
        RecentRawReplacementArtifactHistoryReasonClass,
    pub recent_raw_replacement_artifact_history_explanation: String,
    pub recent_raw_replacement_fixed_path_overwrite_contract: bool,
    pub recent_raw_replacement_fixed_snapshot_path: String,
    pub recent_raw_replacement_fixed_metadata_path: String,
    pub recent_raw_replacement_current_fixed_candidate_exists: bool,
    pub recent_raw_replacement_current_fixed_candidate_manifest_parseable: bool,
    pub recent_raw_replacement_staged_candidate_scan_succeeded: bool,
    pub recent_raw_replacement_staged_candidate_scan_error: Option<String>,
    pub recent_raw_replacement_previous_artifact_archive_path_present: bool,
    pub recent_raw_replacement_previous_artifact_archive_candidate_count: usize,
    pub recent_raw_replacement_previous_artifact_archive_parseable_count: usize,
    pub recent_raw_replacement_previous_artifact_archive_snapshot_paths: Vec<String>,
    pub recent_raw_replacement_previous_artifact_archive_metadata_paths: Vec<String>,
    pub recent_raw_replacement_previous_artifact_history_expected_under_current_contract: bool,
    pub recent_raw_replacement_previous_artifact_history_missing_due_to_unproven_evidence: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawReplacementAttemptTelemetryReasonClass {
    RecentRawReplacementAttemptTelemetryAdvancingButIncomplete,
    RecentRawReplacementAttemptTelemetryStalled,
    RecentRawReplacementAttemptTelemetryResetOrRecreated,
    RecentRawReplacementAttemptTelemetryMissingOrUnparseable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecentRawReplacementAttemptTelemetryDiagnostic {
    pub recent_raw_snapshot_dir: String,
    pub recent_raw_replacement_attempt_telemetry_observed: bool,
    pub recent_raw_replacement_attempt_telemetry_reason_class:
        RecentRawReplacementAttemptTelemetryReasonClass,
    pub recent_raw_replacement_attempt_telemetry_explanation: String,
    pub recent_raw_replacement_attempt_telemetry_probe_bounded: bool,
    pub recent_raw_replacement_attempt_telemetry_probe_mode: String,
    pub recent_raw_replacement_attempt_telemetry_deep_scan_used: bool,
    pub recent_raw_replacement_attempt_telemetry_explicit_paths_checked: Vec<String>,
    pub recent_raw_replacement_attempt_telemetry_scanned_dirs: Vec<String>,
    pub recent_raw_replacement_attempt_telemetry_scan_file_limit: usize,
    pub recent_raw_replacement_attempt_telemetry_scan_truncated: bool,
    pub recent_raw_replacement_attempt_telemetry_artifact_count: usize,
    pub recent_raw_replacement_attempt_telemetry_parseable_count: usize,
    pub recent_raw_replacement_attempt_telemetry_latest_path: Option<String>,
    pub recent_raw_replacement_attempt_telemetry_latest_state: Option<String>,
    pub recent_raw_replacement_attempt_telemetry_latest_timestamp: Option<DateTime<Utc>>,
    pub recent_raw_replacement_attempt_telemetry_proves_advancing: bool,
    pub recent_raw_replacement_attempt_telemetry_proves_stalled: bool,
    pub recent_raw_replacement_attempt_telemetry_proves_reset_or_recreated: bool,
    pub recent_raw_replacement_attempt_telemetry_last_row_count_before: Option<usize>,
    pub recent_raw_replacement_attempt_telemetry_last_row_count_after: Option<usize>,
    pub recent_raw_replacement_attempt_telemetry_last_covered_through_before:
        Option<DiscoveryRuntimeCursor>,
    pub recent_raw_replacement_attempt_telemetry_last_covered_through_after:
        Option<DiscoveryRuntimeCursor>,
    pub recent_raw_replacement_attempt_telemetry_staged_progress_advanced: Option<bool>,
    pub recent_raw_replacement_attempt_telemetry_staged_progress_resumed: Option<bool>,
    pub recent_raw_replacement_attempt_telemetry_staged_progress_preserved_for_retry: Option<bool>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawReplacementConvergenceReasonClass {
    RecentRawReplacementConvergenceReadyToPromote,
    RecentRawReplacementConvergenceAdvancingButIncomplete,
    RecentRawReplacementConvergenceStalledOnLatestAttempt,
    RecentRawReplacementConvergenceResetOrRecreated,
    RecentRawReplacementConvergenceMissingOrUnparseableAttemptTelemetry,
    RecentRawReplacementConvergenceUnprovenDueToMissingEvidence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecentRawReplacementConvergenceDiagnostic {
    pub recent_raw_snapshot_dir: String,
    pub recent_raw_replacement_convergence_observed: bool,
    pub recent_raw_replacement_convergence_reason_class: RecentRawReplacementConvergenceReasonClass,
    pub recent_raw_replacement_convergence_explanation: String,
    pub recent_raw_replacement_candidate_exists: bool,
    pub recent_raw_replacement_candidate_row_count: Option<usize>,
    pub recent_raw_source_row_count: Option<usize>,
    pub recent_raw_replacement_rows_remaining_to_current_source: Option<u64>,
    pub recent_raw_replacement_latest_attempt_row_count_before: Option<usize>,
    pub recent_raw_replacement_latest_attempt_row_count_after: Option<usize>,
    pub recent_raw_replacement_latest_attempt_row_count_delta: Option<i64>,
    pub recent_raw_replacement_latest_attempt_covered_through_before:
        Option<DiscoveryRuntimeCursor>,
    pub recent_raw_replacement_latest_attempt_covered_through_after: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_replacement_latest_attempt_advanced: Option<bool>,
    pub recent_raw_replacement_latest_attempt_resumed: Option<bool>,
    pub recent_raw_replacement_latest_attempt_preserved_for_retry: Option<bool>,
    pub recent_raw_replacement_estimated_attempts_to_current_source: Option<u64>,
    pub recent_raw_replacement_candidate_complete_against_current_source: Option<bool>,
    pub recent_raw_replacement_candidate_promotable_now: Option<bool>,
    pub recent_raw_replacement_attempt_telemetry_path: String,
    pub recent_raw_replacement_attempt_telemetry_parseable: bool,
    pub recent_raw_replacement_attempt_telemetry_probe_bounded: bool,
}
