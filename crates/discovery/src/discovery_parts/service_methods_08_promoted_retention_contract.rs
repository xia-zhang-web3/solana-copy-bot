impl DiscoveryService {
    fn recent_raw_promoted_retained_as_truth_under_current_contract(
        promoted_exists: bool,
        reason_class: RecentRawPromotionBlockerReasonClass,
    ) -> Option<bool> {
        if !promoted_exists {
            return Some(false);
        }
        match reason_class {
            RecentRawPromotionBlockerReasonClass::RecentRawPromotionReadyNow
            | RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByMissingStagedSnapshot
            | RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByIncompleteStagedCoverage
            | RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByStagedNotNewerThanPromoted
            | RecentRawPromotionBlockerReasonClass::RecentRawPromotionNotNeededPromotedAlreadyCurrent => Some(true),
            RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByMissingPromotedLatest => {
                Some(false)
            }
            RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByUnprovenState => None,
        }
    }

    fn classify_recent_raw_promoted_retention_contract(
        promoted_manifest_error: Option<&str>,
        source_window_contract_observed: bool,
        source_window_reason_class: RecentRawSourceWindowContractReasonClass,
        promotion_reason_class: RecentRawPromotionBlockerReasonClass,
        promoted_same_source_db_as_current_source: Option<bool>,
        promoted_currently_retained_as_truth: Option<bool>,
        promoted_start_older_than_current_source: Option<bool>,
        promoted_has_current_contract_invalidation_rule: Option<bool>,
        promoted_invalidated_by_current_source_window_shift: Option<bool>,
        stage3_truth_currently_depends_on_retained_older_promoted_surface: Option<bool>,
        stage3_truth_can_advance_without_new_promotion: Option<bool>,
    ) -> (
        RecentRawPromotedRetentionReasonClass,
        bool,
        RecentRawPromotedRetentionBasis,
        String,
        String,
    ) {
        if promoted_manifest_error.is_some() {
            let explanation = format!(
                "recent_raw promoted retention contract is unproven because the fixed promoted latest surface is partial or invalid (promoted_error={})",
                promoted_manifest_error.unwrap_or("null"),
            );
            return (
                RecentRawPromotedRetentionReasonClass::RecentRawPromotedRetentionContractUnprovenDueToMissingEvidence,
                false,
                RecentRawPromotedRetentionBasis::Unproven,
                explanation.clone(),
                explanation,
            );
        }

        if !source_window_contract_observed {
            let explanation = format!(
                "recent_raw promoted retention contract is unproven because the current source-window relation to promoted latest could not be proven from bounded current evidence (source_window_reason_class={})",
                serde_json::to_string(&source_window_reason_class)
                    .unwrap_or_else(|_| "\"unknown\"".to_string())
                    .trim_matches('"'),
            );
            return (
                RecentRawPromotedRetentionReasonClass::RecentRawPromotedRetentionContractUnprovenDueToMissingEvidence,
                false,
                RecentRawPromotedRetentionBasis::Unproven,
                explanation.clone(),
                explanation,
            );
        }

        match promoted_same_source_db_as_current_source {
            Some(false) => {
                let explanation = "recent_raw promoted retention contract is unproven because the currently promoted latest surface does not point at the same source_db_path as the current source window evidence".to_string();
                return (
                    RecentRawPromotedRetentionReasonClass::RecentRawPromotedRetentionContractUnprovenDueToMissingEvidence,
                    false,
                    RecentRawPromotedRetentionBasis::Unproven,
                    explanation.clone(),
                    explanation,
                );
            }
            None => {
                let explanation = "recent_raw promoted retention contract is unproven because the promoted/source source_db_path relation could not both be proven from current artifacts".to_string();
                return (
                    RecentRawPromotedRetentionReasonClass::RecentRawPromotedRetentionContractUnprovenDueToMissingEvidence,
                    false,
                    RecentRawPromotedRetentionBasis::Unproven,
                    explanation.clone(),
                    explanation,
                );
            }
            Some(true) => {}
        }

        match promoted_currently_retained_as_truth {
            Some(false) => {
                let explanation = "recent_raw promoted retention contract is unproven because no currently retained promoted latest truth surface is present at the fixed latest paths".to_string();
                return (
                    RecentRawPromotedRetentionReasonClass::RecentRawPromotedRetentionContractUnprovenDueToMissingEvidence,
                    false,
                    RecentRawPromotedRetentionBasis::Unproven,
                    explanation.clone(),
                    explanation,
                );
            }
            None => {
                let explanation = format!(
                    "recent_raw promoted retention contract is unproven because the current promotion contract result did not prove whether promoted latest is still retained as truth (promotion_reason_class={})",
                    serde_json::to_string(&promotion_reason_class)
                        .unwrap_or_else(|_| "\"unknown\"".to_string())
                        .trim_matches('"'),
                );
                return (
                    RecentRawPromotedRetentionReasonClass::RecentRawPromotedRetentionContractUnprovenDueToMissingEvidence,
                    false,
                    RecentRawPromotedRetentionBasis::Unproven,
                    explanation.clone(),
                    explanation,
                );
            }
            Some(true) => {}
        }

        if promoted_start_older_than_current_source == Some(false)
            && promoted_has_current_contract_invalidation_rule == Some(false)
            && promoted_invalidated_by_current_source_window_shift == Some(false)
            && stage3_truth_can_advance_without_new_promotion == Some(true)
        {
            let explanation = "current bounded source-window evidence shows that promoted latest and the current source window still start at the same covered_since on the same source path. Under the current recent_raw contract, the fixed promoted latest surface remains the active Stage 3 truth and no replacement promotion is required for truth to advance.".to_string();
            let basis_explanation = "the fixed promoted latest surface still matches the current source start, so current code keeps it as Stage 3 truth without requiring a replacement write".to_string();
            return (
                RecentRawPromotedRetentionReasonClass::RecentRawPromotedCurrentTruthMatchesCurrentSource,
                true,
                RecentRawPromotedRetentionBasis::MatchesCurrentSourceStart,
                explanation,
                basis_explanation,
            );
        }

        if promoted_start_older_than_current_source == Some(true)
            && promoted_has_current_contract_invalidation_rule == Some(false)
            && promoted_invalidated_by_current_source_window_shift == Some(false)
            && stage3_truth_currently_depends_on_retained_older_promoted_surface == Some(true)
            && stage3_truth_can_advance_without_new_promotion == Some(false)
        {
            let explanation = format!(
                "current bounded source-window evidence shows that the current source covered_since has moved later than promoted latest on the same source path, but the current recent_raw promotion contract still retains the fixed promoted latest surface as truth. Under current code this window shift changes promotion/readiness state (promotion_reason_class={}) and blocks Stage 3 truth from advancing without a replacement promotion, but it does not invalidate or retire the existing promoted latest surface on its own.",
                serde_json::to_string(&promotion_reason_class)
                    .unwrap_or_else(|_| "\"unknown\"".to_string())
                    .trim_matches('"'),
            );
            let basis_explanation = "the fixed promoted latest surface remains retained until a replacement write/promotion updates the latest paths; the current source-window shift only changes promotion/readiness state and does not retire promoted latest by itself".to_string();
            return (
                RecentRawPromotedRetentionReasonClass::RecentRawPromotedRetainedByDesignDespiteOlderWindow,
                true,
                RecentRawPromotedRetentionBasis::FixedPromotedLatestRetainedUntilReplacement,
                explanation,
                basis_explanation,
            );
        }

        let explanation = format!(
            "recent_raw promoted retention contract remains unproven from current artifacts: promoted_start_older_than_current_source={}, promoted_has_current_contract_invalidation_rule={}, promoted_invalidated_by_current_source_window_shift={}, stage3_truth_depends_on_retained_older_promoted_surface={}, stage3_truth_can_advance_without_new_promotion={}, promotion_reason_class={}, source_window_reason_class={}",
            promoted_start_older_than_current_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            promoted_has_current_contract_invalidation_rule
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            promoted_invalidated_by_current_source_window_shift
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            stage3_truth_currently_depends_on_retained_older_promoted_surface
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            stage3_truth_can_advance_without_new_promotion
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            serde_json::to_string(&promotion_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"'),
            serde_json::to_string(&source_window_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"'),
        );
        let basis_explanation = "current artifacts do not reduce the promoted-latest retention contract to one exact proven relation without inference".to_string();
        (
            RecentRawPromotedRetentionReasonClass::RecentRawPromotedRetentionContractUnprovenDueToMissingEvidence,
            false,
            RecentRawPromotedRetentionBasis::Unproven,
            explanation,
            basis_explanation,
        )
    }

}
