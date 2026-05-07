use super::*;

impl DiscoveryService {
    pub(super) fn classify_recent_raw_replacement_promotion_contract(
        promoted_manifest_error: Option<&str>,
        staged_manifest_error: Option<&str>,
        promoted_exists: bool,
        current_source_state_available: bool,
        replacement_candidate_exists: bool,
        candidate_source_db_matches_promoted: Option<bool>,
        candidate_start_matches_current_source: Option<bool>,
        candidate_covered_through_relation_to_promoted: RecentRawLineageRelation,
        candidate_row_count_relation_to_promoted: RecentRawLineageRelation,
        candidate_complete_against_current_source: Option<bool>,
        candidate_promotable_now: Option<bool>,
        stage3_blocked_on_replacement_candidate: Option<bool>,
        replacement_promotion_would_retire_current_promoted_truth: Option<bool>,
        promotion_reason_class: RecentRawPromotionBlockerReasonClass,
    ) -> (RecentRawReplacementPromotionReasonClass, bool, String) {
        if promoted_manifest_error.is_some() || staged_manifest_error.is_some() {
            return (
                RecentRawReplacementPromotionReasonClass::RecentRawReplacementPromotionContractUnprovenDueToMissingEvidence,
                false,
                format!(
                    "recent_raw replacement-promotion contract is unproven because one or more snapshot surfaces are partial or invalid (promoted_error={}, staged_error={})",
                    promoted_manifest_error.unwrap_or("null"),
                    staged_manifest_error.unwrap_or("null"),
                ),
            );
        }

        if !promoted_exists {
            return (
                RecentRawReplacementPromotionReasonClass::RecentRawReplacementPromotionContractUnprovenDueToMissingEvidence,
                false,
                "recent_raw replacement-promotion contract is unproven because no current promoted latest truth surface is present to replace".to_string(),
            );
        }

        if !replacement_candidate_exists {
            return (
                RecentRawReplacementPromotionReasonClass::RecentRawReplacementCandidateMissing,
                true,
                "recent_raw replacement promotion is blocked because no fixed staged replacement candidate is currently present at the staged paths, so current code has nothing available to promote in place of the retained promoted latest surface".to_string(),
            );
        }

        if !current_source_state_available {
            return (
                RecentRawReplacementPromotionReasonClass::RecentRawReplacementPromotionContractUnprovenDueToMissingEvidence,
                false,
                "recent_raw replacement-promotion contract is unproven because the current source recent_raw journal state could not be proven read-only from the runtime db".to_string(),
            );
        }

        match candidate_source_db_matches_promoted {
            Some(false) => {
                return (
                    RecentRawReplacementPromotionReasonClass::RecentRawReplacementPromotionContractUnprovenDueToMissingEvidence,
                    false,
                    "recent_raw replacement-promotion contract is unproven because the staged replacement candidate does not point at the same source_db_path as promoted latest".to_string(),
                );
            }
            None => {
                return (
                    RecentRawReplacementPromotionReasonClass::RecentRawReplacementPromotionContractUnprovenDueToMissingEvidence,
                    false,
                    "recent_raw replacement-promotion contract is unproven because the staged/promoted source_db_path relation could not both be proven from current manifests".to_string(),
                );
            }
            Some(true) => {}
        }

        if candidate_complete_against_current_source == Some(false) {
            return (
                RecentRawReplacementPromotionReasonClass::RecentRawReplacementCandidateIncompleteAgainstCurrentSource,
                true,
                format!(
                    "recent_raw replacement promotion is currently blocked because the fixed staged candidate is still incomplete against the current source recent_raw journal frontier. candidate_start_matches_current_source={}, covered_through_relation_to_promoted={}, row_count_relation_to_promoted={}, promotion_reason_class={}",
                    candidate_start_matches_current_source
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    serde_json::to_string(&candidate_covered_through_relation_to_promoted)
                        .unwrap_or_else(|_| "\"unknown\"".to_string())
                        .trim_matches('"'),
                    serde_json::to_string(&candidate_row_count_relation_to_promoted)
                        .unwrap_or_else(|_| "\"unknown\"".to_string())
                        .trim_matches('"'),
                    serde_json::to_string(&promotion_reason_class)
                        .unwrap_or_else(|_| "\"unknown\"".to_string())
                        .trim_matches('"'),
                ),
            );
        }

        if candidate_promotable_now == Some(true) {
            return (
                RecentRawReplacementPromotionReasonClass::RecentRawReplacementCandidateReadyToPromote,
                true,
                format!(
                    "recent_raw replacement candidate is complete against the current source frontier and promotable now under current code. candidate_start_matches_current_source={}, covered_through_relation_to_promoted={}, row_count_relation_to_promoted={}, replacement_promotion_would_retire_current_promoted_truth={}",
                    candidate_start_matches_current_source
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    serde_json::to_string(&candidate_covered_through_relation_to_promoted)
                        .unwrap_or_else(|_| "\"unknown\"".to_string())
                        .trim_matches('"'),
                    serde_json::to_string(&candidate_row_count_relation_to_promoted)
                        .unwrap_or_else(|_| "\"unknown\"".to_string())
                        .trim_matches('"'),
                    replacement_promotion_would_retire_current_promoted_truth
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                ),
            );
        }

        if candidate_complete_against_current_source == Some(true)
            && matches!(
                promotion_reason_class,
                RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByStagedNotNewerThanPromoted
                    | RecentRawPromotionBlockerReasonClass::RecentRawPromotionNotNeededPromotedAlreadyCurrent
            )
        {
            return (
                RecentRawReplacementPromotionReasonClass::RecentRawReplacementCandidateNotNewerThanPromoted,
                true,
                format!(
                    "recent_raw replacement promotion is currently blocked because the staged candidate is complete against current source but is not newer than promoted latest on the contract dimensions current code compares before replacement. candidate_start_matches_current_source={}, covered_through_relation_to_promoted={}, row_count_relation_to_promoted={}, stage3_blocked_on_replacement_candidate={}",
                    candidate_start_matches_current_source
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                    serde_json::to_string(&candidate_covered_through_relation_to_promoted)
                        .unwrap_or_else(|_| "\"unknown\"".to_string())
                        .trim_matches('"'),
                    serde_json::to_string(&candidate_row_count_relation_to_promoted)
                        .unwrap_or_else(|_| "\"unknown\"".to_string())
                        .trim_matches('"'),
                    stage3_blocked_on_replacement_candidate
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "null".to_string()),
                ),
            );
        }

        (
            RecentRawReplacementPromotionReasonClass::RecentRawReplacementPromotionContractUnprovenDueToMissingEvidence,
            false,
            format!(
                "recent_raw replacement-promotion contract remains unproven from current artifacts: candidate_complete_against_current_source={}, candidate_promotable_now={}, candidate_start_matches_current_source={}, covered_through_relation_to_promoted={}, row_count_relation_to_promoted={}, promotion_reason_class={}, replacement_promotion_would_retire_current_promoted_truth={}, stage3_blocked_on_replacement_candidate={}",
                candidate_complete_against_current_source
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                candidate_promotable_now
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                candidate_start_matches_current_source
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                serde_json::to_string(&candidate_covered_through_relation_to_promoted)
                    .unwrap_or_else(|_| "\"unknown\"".to_string())
                    .trim_matches('"'),
                serde_json::to_string(&candidate_row_count_relation_to_promoted)
                    .unwrap_or_else(|_| "\"unknown\"".to_string())
                    .trim_matches('"'),
                serde_json::to_string(&promotion_reason_class)
                    .unwrap_or_else(|_| "\"unknown\"".to_string())
                    .trim_matches('"'),
                replacement_promotion_would_retire_current_promoted_truth
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
                stage3_blocked_on_replacement_candidate
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "null".to_string()),
            ),
        )
    }
}
