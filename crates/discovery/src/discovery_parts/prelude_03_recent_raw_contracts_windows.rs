use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawStagedWindowSeedingBasis {
    MatchesPromotedStart,
    MatchesCurrentSourceStart,
    MatchesBothPromotedAndCurrentSourceStart,
    MatchesNeitherPromotedNorCurrentSourceStart,
    Unproven,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecentRawStagedWindowSeedingDiagnostic {
    pub recent_raw_snapshot_dir: String,
    pub recent_raw_staged_window_seeding_observed: bool,
    pub recent_raw_staged_window_seeding_reason_class: RecentRawStagedWindowSeedingReasonClass,
    pub recent_raw_staged_window_seeding_explanation: String,
    pub recent_raw_staged_window_historical_seeding_basis_proven_from_current_artifacts: bool,
    pub recent_raw_promoted_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_staged_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_source_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_promoted_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_staged_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_source_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_staged_start_matches_promoted_start: Option<bool>,
    pub recent_raw_staged_start_matches_source_start: Option<bool>,
    pub recent_raw_staged_start_matches_current_window_cutoff: Option<bool>,
    pub recent_raw_staged_start_matches_neither_promoted_nor_source: Option<bool>,
    pub recent_raw_staged_start_current_evidence_basis: RecentRawStagedWindowSeedingBasis,
    pub recent_raw_staged_start_current_evidence_explanation: String,
    pub recent_raw_staged_same_source_db_as_promoted: Option<bool>,
    pub recent_raw_staged_created_after_promoted: Option<bool>,
    pub recent_raw_staged_created_at_matches_promoted_created_at: Option<bool>,
    pub recent_raw_promoted_can_seed_staged_progress_under_current_code: Option<bool>,
    pub recent_raw_promoted_seed_blocked_by_source_contract_mismatch: Option<bool>,
    pub recent_raw_promoted_supersedes_staged_progress: Option<bool>,
    pub recent_raw_staged_manifest_matches_sqlite_content: Option<bool>,
    pub recent_raw_staged_manifest_sqlite_match_unproven: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawSourceWindowContractReasonClass {
    RecentRawSourceWindowCurrentStartMatchesPromotedStart,
    RecentRawSourceWindowCurrentContractExcludesOlderRows,
    RecentRawSourceWindowPromotedSurfaceReflectsOlderWindow,
    RecentRawSourceWindowCurrentAndPromotedContractRelationUnproven,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawSourceWindowContractBasis {
    MatchesPromotedStart,
    CurrentSourceObservedSwapsWindowExcludesOlderRows,
    PromotedStillReflectsOlderWindowWhileCurrentSourceStartsLater,
    Unproven,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecentRawSourceWindowContractDiagnostic {
    pub recent_raw_snapshot_dir: String,
    pub recent_raw_source_window_contract_observed: bool,
    pub recent_raw_source_window_contract_reason_class: RecentRawSourceWindowContractReasonClass,
    pub recent_raw_source_window_contract_explanation: String,
    pub recent_raw_source_window_probe_bounded: bool,
    pub recent_raw_source_window_probe_mode: RecentRawSourceWindowProbeMode,
    pub recent_raw_source_window_probe_explanation: String,
    pub recent_raw_promoted_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_source_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_source_bounded_probe_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_source_scanned_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_promoted_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_source_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_source_bounded_probe_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_source_scanned_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_source_start_later_than_promoted: Option<bool>,
    pub recent_raw_source_contract_currently_excludes_older_rows: Option<bool>,
    pub recent_raw_source_window_matches_current_bounded_contract: Option<bool>,
    pub recent_raw_promoted_reflects_older_still_promoted_window: Option<bool>,
    pub recent_raw_source_window_contract_basis: RecentRawSourceWindowContractBasis,
    pub recent_raw_source_window_contract_basis_explanation: String,
    pub recent_raw_source_same_source_db_as_promoted: Option<bool>,
    pub recent_raw_source_cached_state_matches_bounded_probe: Option<bool>,
    pub recent_raw_source_cached_state_matches_scanned_rows: Option<bool>,
    pub recent_raw_source_row_count: Option<usize>,
    pub recent_raw_source_scanned_row_count: Option<usize>,
    pub recent_raw_source_last_pruned_rows: Option<usize>,
    pub recent_raw_source_last_pruned_at: Option<DateTime<Utc>>,
    pub recent_raw_source_prune_activity_recorded: Option<bool>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawSourceWindowProbeMode {
    BoundedIndexEdges,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawPromotedRetentionReasonClass {
    RecentRawPromotedCurrentTruthMatchesCurrentSource,
    RecentRawPromotedRetainedByDesignDespiteOlderWindow,
    RecentRawPromotedRetentionContractUnprovenDueToMissingEvidence,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecentRawPromotedRetentionBasis {
    MatchesCurrentSourceStart,
    FixedPromotedLatestRetainedUntilReplacement,
    Unproven,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecentRawPromotedRetentionContractDiagnostic {
    pub recent_raw_snapshot_dir: String,
    pub recent_raw_promoted_snapshot_path: String,
    pub recent_raw_promoted_metadata_path: String,
    pub recent_raw_promoted_retention_observed: bool,
    pub recent_raw_promoted_retention_reason_class: RecentRawPromotedRetentionReasonClass,
    pub recent_raw_promoted_retention_explanation: String,
    pub recent_raw_promoted_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_source_covered_since: Option<DateTime<Utc>>,
    pub recent_raw_promoted_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_source_covered_through: Option<DiscoveryRuntimeCursor>,
    pub recent_raw_promoted_start_older_than_current_source: Option<bool>,
    pub recent_raw_promoted_same_source_db_as_current_source: Option<bool>,
    pub recent_raw_promoted_currently_retained_as_truth: Option<bool>,
    pub recent_raw_promoted_has_current_contract_invalidation_rule: Option<bool>,
    pub recent_raw_promoted_invalidated_by_current_source_window_shift: Option<bool>,
    pub recent_raw_promoted_retention_basis: RecentRawPromotedRetentionBasis,
    pub recent_raw_promoted_retention_basis_explanation: String,
    pub recent_raw_stage3_truth_currently_depends_on_retained_older_promoted_surface: Option<bool>,
    pub recent_raw_stage3_truth_can_advance_without_new_promotion: Option<bool>,
    pub recent_raw_promoted_exists: bool,
    pub recent_raw_promoted_snapshot_present: bool,
    pub recent_raw_promoted_metadata_present: bool,
    pub recent_raw_promotion_ready_now: bool,
    pub recent_raw_promotion_reason_class: RecentRawPromotionBlockerReasonClass,
    pub recent_raw_stage3_truth_blocked_by_promotion: bool,
    pub recent_raw_source_window_contract_observed: bool,
    pub recent_raw_source_window_contract_reason_class: RecentRawSourceWindowContractReasonClass,
}
