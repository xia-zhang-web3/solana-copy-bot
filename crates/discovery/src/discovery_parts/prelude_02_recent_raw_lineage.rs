#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawStagedLineageReasonClass {
    RecentRawStagedLineageMonotonicButIncomplete,
    RecentRawStagedLineageRegressedRelativeToPromoted,
    RecentRawStagedLineagePointsToDifferentSource,
    RecentRawStagedLineageUnprovenDueToMissingEvidence,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawLineageRelation {
    Ahead,
    Equal,
    Behind,
    DifferentSource,
    Unproven,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawCursorRelationBasis {
    DirectCoveredThroughCursorComparison,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecentRawStagedLineageDiagnostic {
    pub recent_raw_snapshot_dir: String,
    pub recent_raw_staged_lineage_observed: bool,
    pub recent_raw_staged_lineage_reason_class: RecentRawStagedLineageReasonClass,
    pub recent_raw_staged_lineage_explanation: String,
    pub recent_raw_promoted_source_db_path: Option<String>,
    pub recent_raw_promoted_created_at: Option<DateTime<Utc>>,
    pub recent_raw_promoted_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_promoted_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_promoted_row_count: Option<usize>,
    pub recent_raw_promoted_last_batch_completed_at: Option<DateTime<Utc>>,
    pub recent_raw_staged_source_db_path: Option<String>,
    pub recent_raw_staged_created_at: Option<DateTime<Utc>>,
    pub recent_raw_staged_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_staged_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_staged_row_count: Option<usize>,
    pub recent_raw_staged_last_batch_completed_at: Option<DateTime<Utc>>,
    pub recent_raw_source_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_source_row_count: Option<usize>,
    pub recent_raw_source_outruns_staged: Option<bool>,
    pub recent_raw_source_outruns_promoted: Option<bool>,
    pub recent_raw_source_outruns_both: Option<bool>,
    pub recent_raw_staged_same_source_db_as_promoted: Option<bool>,
    pub recent_raw_staged_cursor_relation_basis: RecentRawCursorRelationBasis,
    pub recent_raw_staged_cursor_relation_explanation: String,
    pub recent_raw_staged_cursor_relation_to_promoted: RecentRawLineageRelation,
    pub recent_raw_staged_cursor_ts_relation_to_promoted: RecentRawLineageRelation,
    pub recent_raw_staged_cursor_slot_relation_to_promoted: RecentRawLineageRelation,
    pub recent_raw_staged_cursor_signature_equal_to_promoted: Option<bool>,
    pub recent_raw_staged_row_count_relation_to_promoted: RecentRawLineageRelation,
    pub recent_raw_staged_covered_since_relation_to_promoted: RecentRawLineageRelation,
    pub recent_raw_staged_monotonic_relative_to_promoted: Option<bool>,
    pub recent_raw_staged_regressed_relative_to_promoted: Option<bool>,
    pub recent_raw_staged_closer_to_source_than_promoted: Option<bool>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawStagedRegressionReasonClass {
    RecentRawStagedRegressionSelectedOlderStagedArtifact,
    RecentRawStagedRegressionMultipleCandidatesWrongSelection,
    RecentRawStagedRegressionArtifactItselfAlreadyBehind,
    RecentRawStagedRegressionUnprovenDueToMissingEvidence,
    RecentRawStagedRegressionNoCurrentSameSourceRegressionObserved,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecentRawStagedRegressionDiagnostic {
    pub recent_raw_snapshot_dir: String,
    pub recent_raw_staged_regression_observed: bool,
    pub recent_raw_staged_regression_reason_class: RecentRawStagedRegressionReasonClass,
    pub recent_raw_staged_regression_explanation: String,
    pub recent_raw_promoted_source_db_path: Option<String>,
    pub recent_raw_promoted_snapshot_path: String,
    pub recent_raw_promoted_metadata_path: String,
    pub recent_raw_promoted_created_at: Option<DateTime<Utc>>,
    pub recent_raw_promoted_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_promoted_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_promoted_row_count: Option<usize>,
    pub recent_raw_promoted_last_batch_completed_at: Option<DateTime<Utc>>,
    pub recent_raw_staged_source_db_path: Option<String>,
    pub recent_raw_staged_snapshot_path: String,
    pub recent_raw_staged_metadata_path: String,
    pub recent_raw_staged_created_at: Option<DateTime<Utc>>,
    pub recent_raw_staged_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_staged_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_staged_row_count: Option<usize>,
    pub recent_raw_staged_last_batch_completed_at: Option<DateTime<Utc>>,
    pub recent_raw_staged_candidate_count: usize,
    pub recent_raw_multiple_staged_candidates_present: bool,
    pub recent_raw_staged_candidate_snapshot_paths: Vec<String>,
    pub recent_raw_staged_candidate_metadata_paths: Vec<String>,
    pub recent_raw_selected_staged_is_latest_candidate: Option<bool>,
    pub recent_raw_selected_staged_created_after_promoted: Option<bool>,
    pub recent_raw_selected_staged_frontier_behind_promoted_before_comparison: Option<bool>,
    pub recent_raw_staged_selection_reason: String,
    pub recent_raw_selected_staged_completed_after_creation: Option<bool>,
    pub recent_raw_staged_same_source_db_as_promoted: Option<bool>,
    pub recent_raw_selected_staged_cursor_relation_to_promoted: RecentRawLineageRelation,
    pub recent_raw_selected_staged_row_count_relation_to_promoted: RecentRawLineageRelation,
    pub recent_raw_selected_staged_covered_since_relation_to_promoted: RecentRawLineageRelation,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawStagedBirthReasonClass {
    RecentRawStagedCurrentArtifactLaterStartWindowObserved,
    RecentRawStagedCurrentArtifactManifestAndSqliteContentAgreeButBehind,
    RecentRawStagedCurrentArtifactManifestSqliteMismatch,
    RecentRawStagedCurrentArtifactUnprovenDueToMissingEvidence,
    RecentRawStagedCurrentArtifactNotCurrentlyBehindPromoted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecentRawStagedBirthDiagnostic {
    pub recent_raw_snapshot_dir: String,
    pub recent_raw_staged_birth_observed: bool,
    pub recent_raw_staged_birth_proven_from_current_artifacts: bool,
    pub recent_raw_staged_birth_reason_class: RecentRawStagedBirthReasonClass,
    pub recent_raw_staged_birth_explanation: String,
    pub recent_raw_staged_birth_snapshot_path: String,
    pub recent_raw_staged_birth_metadata_path: String,
    pub recent_raw_staged_birth_created_at: Option<DateTime<Utc>>,
    pub recent_raw_staged_birth_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_staged_birth_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_staged_birth_row_count: Option<usize>,
    pub recent_raw_promoted_created_at: Option<DateTime<Utc>>,
    pub recent_raw_promoted_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_promoted_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_promoted_row_count: Option<usize>,
    pub recent_raw_staged_birth_created_after_promoted: Option<bool>,
    pub recent_raw_staged_birth_covered_since_relation_to_promoted: RecentRawLineageRelation,
    pub recent_raw_staged_birth_covered_through_relation_to_promoted: RecentRawLineageRelation,
    pub recent_raw_staged_birth_row_count_relation_to_promoted: RecentRawLineageRelation,
    pub recent_raw_staged_birth_window_later_start_than_promoted: Option<bool>,
    pub recent_raw_staged_birth_window_narrower_or_older_than_promoted: Option<bool>,
    pub recent_raw_staged_birth_same_source_db_as_promoted: Option<bool>,
    pub recent_raw_staged_birth_source_outruns_both: Option<bool>,
    pub recent_raw_staged_birth_manifest_matches_sqlite_content: Option<bool>,
    pub recent_raw_staged_birth_manifest_sqlite_match_unproven: bool,
    pub recent_raw_staged_birth_sqlite_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_staged_birth_sqlite_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_staged_birth_sqlite_row_count: Option<usize>,
    pub recent_raw_staged_birth_sqlite_last_batch_completed_at: Option<DateTime<Utc>>,
    pub recent_raw_staged_birth_sqlite_inspection_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawStagedWindowSeedingReasonClass {
    RecentRawStagedWindowCurrentStartMatchesPromotedStart,
    RecentRawStagedWindowCurrentStartMatchesSourceStart,
    RecentRawStagedWindowCurrentStartMatchesBothPromotedAndSourceStart,
    RecentRawStagedWindowCurrentStartMatchesNeitherPromotedNorSource,
    RecentRawStagedWindowCurrentEvidenceUnprovenDueToMissingEvidence,
}
