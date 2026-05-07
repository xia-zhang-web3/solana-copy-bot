use super::*;

impl DiscoveryService {
    pub(super) fn classify_recent_raw_replacement_progress_contract(
        staged_manifest_error: Option<&str>,
        replacement_candidate_exists: bool,
        replacement_candidate_complete_against_current_source: Option<bool>,
        replacement_promotion_reason_class: RecentRawReplacementPromotionReasonClass,
        replacement_manifest_matches_sqlite_content: Option<bool>,
        previous_candidate_exists: bool,
        candidate_completed_after_creation: Option<bool>,
        same_identity_as_previous_attempt: Option<bool>,
        row_count_advanced: Option<bool>,
        covered_through_advanced: Option<bool>,
        last_batch_completed_at_advanced: Option<bool>,
        candidate_advancing: Option<bool>,
        candidate_stalled: Option<bool>,
        candidate_reset_or_recreated: Option<bool>,
    ) -> (RecentRawReplacementProgressReasonClass, bool, String) {
        if staged_manifest_error.is_some() {
            return (
                RecentRawReplacementProgressReasonClass::RecentRawReplacementProgressUnprovenDueToMissingEvidence,
                false,
                format!(
                    "recent_raw replacement progress is unproven because the fixed staged replacement candidate manifest is partial or invalid (staged_error={})",
                    staged_manifest_error.unwrap_or("null"),
                ),
            );
        }

        if !replacement_candidate_exists {
            return (
                RecentRawReplacementProgressReasonClass::RecentRawReplacementProgressUnprovenDueToMissingEvidence,
                false,
                "recent_raw replacement progress is unproven because no fixed staged replacement candidate is currently present at the staged paths".to_string(),
            );
        }

        if replacement_candidate_complete_against_current_source != Some(false)
            || replacement_promotion_reason_class
                != RecentRawReplacementPromotionReasonClass::RecentRawReplacementCandidateIncompleteAgainstCurrentSource
        {
            return (
                RecentRawReplacementProgressReasonClass::RecentRawReplacementProgressUnprovenDueToMissingEvidence,
                false,
                format!(
                    "recent_raw replacement progress is unproven because the current fixed staged candidate is not proven incomplete against current source under the replacement-promotion contract. replacement_candidate_complete_against_current_source={}, replacement_promotion_reason_class={}",
                    replacement_candidate_complete_against_current_source
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    serde_json::to_string(&replacement_promotion_reason_class)
                        .unwrap_or_else(|_| "\"unknown\"".to_string())
                        .trim_matches('"'),
                ),
            );
        }

        if replacement_manifest_matches_sqlite_content == Some(false) {
            return (
                RecentRawReplacementProgressReasonClass::RecentRawReplacementProgressUnprovenDueToMissingEvidence,
                false,
                "recent_raw replacement progress is unproven because the fixed staged manifest does not currently agree with the staged sqlite content".to_string(),
            );
        }

        if replacement_manifest_matches_sqlite_content.is_none() {
            return (
                RecentRawReplacementProgressReasonClass::RecentRawReplacementProgressUnprovenDueToMissingEvidence,
                false,
                "recent_raw replacement progress is unproven because the fixed staged sqlite content could not be read read-only to validate the current manifest frontier".to_string(),
            );
        }

        if !previous_candidate_exists && candidate_completed_after_creation == Some(true) {
            return (
                RecentRawReplacementProgressReasonClass::RecentRawReplacementProgressUnprovenDueToMissingEvidence,
                false,
                "recent_raw replacement progress is unproven because the current fixed staged candidate was updated after creation, but no previous persisted replacement candidate artifact is available for cross-attempt comparison".to_string(),
            );
        }

        if candidate_reset_or_recreated == Some(true) {
            return (
                RecentRawReplacementProgressReasonClass::RecentRawReplacementProgressResetOrRecreated,
                true,
                format!(
                    "recent_raw replacement progress evidence shows that the fixed staged candidate no longer matches the previous persisted replacement identity. previous_candidate_exists={}, same_identity_as_previous_attempt={}, row_count_advanced={}, covered_through_advanced={}, last_batch_completed_at_advanced={}",
                    previous_candidate_exists,
                    same_identity_as_previous_attempt
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    row_count_advanced
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    covered_through_advanced
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    last_batch_completed_at_advanced
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                ),
            );
        }

        if candidate_stalled == Some(true) && same_identity_as_previous_attempt == Some(true) {
            return (
                RecentRawReplacementProgressReasonClass::RecentRawReplacementProgressStalledOnSameCandidate,
                true,
                format!(
                    "recent_raw replacement progress evidence shows that the fixed staged candidate is still the same continuing candidate as the previous persisted attempt artifact, but its row_count, covered_through, and last_batch_completed_at have not advanced. row_count_advanced={}, covered_through_advanced={}, last_batch_completed_at_advanced={}",
                    row_count_advanced
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    covered_through_advanced
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    last_batch_completed_at_advanced
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                ),
            );
        }

        if candidate_advancing == Some(true) && same_identity_as_previous_attempt == Some(true) {
            return (
                RecentRawReplacementProgressReasonClass::RecentRawReplacementProgressAdvancingButIncomplete,
                true,
                format!(
                    "recent_raw replacement progress evidence shows that the fixed staged candidate is continuing across attempts and has advanced while still remaining incomplete against current source. previous_candidate_exists={}, row_count_advanced={}, covered_through_advanced={}, last_batch_completed_at_advanced={}",
                    previous_candidate_exists,
                    row_count_advanced
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    covered_through_advanced
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    last_batch_completed_at_advanced
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                ),
            );
        }

        (
            RecentRawReplacementProgressReasonClass::RecentRawReplacementProgressUnprovenDueToMissingEvidence,
            false,
            format!(
                "recent_raw replacement progress remains unproven from current artifacts: previous_candidate_exists={}, candidate_completed_after_creation={}, same_identity_as_previous_attempt={}, row_count_advanced={}, covered_through_advanced={}, last_batch_completed_at_advanced={}, candidate_advancing={}, candidate_stalled={}, candidate_reset_or_recreated={}",
                previous_candidate_exists,
                candidate_completed_after_creation
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                same_identity_as_previous_attempt
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                row_count_advanced
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                covered_through_advanced
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                last_batch_completed_at_advanced
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                candidate_advancing
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                candidate_stalled
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                candidate_reset_or_recreated
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
            ),
        )
    }
}
