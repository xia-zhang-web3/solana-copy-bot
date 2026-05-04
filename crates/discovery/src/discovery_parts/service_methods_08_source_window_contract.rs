impl DiscoveryService {
    fn classify_recent_raw_source_window_contract(
        promoted_exists: bool,
        promoted_manifest_error: Option<&str>,
        current_source_state_available: bool,
        same_source_db_as_promoted: Option<bool>,
        source_window_matches_current_bounded_contract: Option<bool>,
        source_start_matches_promoted_start: Option<bool>,
        source_start_later_than_promoted: Option<bool>,
        source_contract_currently_excludes_older_rows: Option<bool>,
        promoted_reflects_older_still_promoted_window: Option<bool>,
        source_prune_activity_recorded: Option<bool>,
        source_scan_error: Option<&str>,
    ) -> (
        RecentRawSourceWindowContractReasonClass,
        bool,
        RecentRawSourceWindowContractBasis,
        String,
        String,
    ) {
        if promoted_manifest_error.is_some() {
            let explanation = format!(
                "recent_raw source window contract evidence is unproven because the promoted latest surface is partial or invalid (promoted_error={})",
                promoted_manifest_error.unwrap_or("null"),
            );
            return (
                RecentRawSourceWindowContractReasonClass::RecentRawSourceWindowCurrentAndPromotedContractRelationUnproven,
                false,
                RecentRawSourceWindowContractBasis::Unproven,
                explanation.clone(),
                explanation,
            );
        }

        if !promoted_exists || !current_source_state_available {
            let explanation = format!(
                "recent_raw source window contract evidence is unproven because promoted_exists={} and source_state_available={}",
                promoted_exists, current_source_state_available
            );
            return (
                RecentRawSourceWindowContractReasonClass::RecentRawSourceWindowCurrentAndPromotedContractRelationUnproven,
                false,
                RecentRawSourceWindowContractBasis::Unproven,
                explanation.clone(),
                explanation,
            );
        }

        match same_source_db_as_promoted {
            Some(false) => {
                let explanation = "recent_raw source window contract evidence is unproven because the current runtime db path does not match promoted latest source_db_path".to_string();
                return (
                    RecentRawSourceWindowContractReasonClass::RecentRawSourceWindowCurrentAndPromotedContractRelationUnproven,
                    false,
                    RecentRawSourceWindowContractBasis::Unproven,
                    explanation.clone(),
                    explanation,
                );
            }
            None => {
                let explanation = "recent_raw source window contract evidence is unproven because the current runtime db path and promoted source_db_path could not both be proven".to_string();
                return (
                    RecentRawSourceWindowContractReasonClass::RecentRawSourceWindowCurrentAndPromotedContractRelationUnproven,
                    false,
                    RecentRawSourceWindowContractBasis::Unproven,
                    explanation.clone(),
                    explanation,
                );
            }
            Some(true) => {}
        }

        match source_window_matches_current_bounded_contract {
            Some(false) => {
                let explanation = "recent_raw source window contract evidence is unproven because the cached recent_raw_journal_state does not match the bounded oldest/newest observed_swaps probe".to_string();
                return (
                    RecentRawSourceWindowContractReasonClass::RecentRawSourceWindowCurrentAndPromotedContractRelationUnproven,
                    false,
                    RecentRawSourceWindowContractBasis::Unproven,
                    explanation.clone(),
                    explanation,
                );
            }
            None => {
                let explanation = format!(
                    "recent_raw source window contract evidence is unproven because the bounded observed_swaps edge probe could not be proven read-only{}",
                    source_scan_error
                        .map(|error| format!(" ({error})"))
                        .unwrap_or_default()
                );
                return (
                    RecentRawSourceWindowContractReasonClass::RecentRawSourceWindowCurrentAndPromotedContractRelationUnproven,
                    false,
                    RecentRawSourceWindowContractBasis::Unproven,
                    explanation.clone(),
                    explanation,
                );
            }
            Some(true) => {}
        }

        if source_start_matches_promoted_start == Some(true) {
            let explanation = "current artifacts show that the cached recent_raw_journal_state matches the bounded oldest/newest observed_swaps probe and that both start at the same covered_since as promoted latest on the same source path. This proves the current source bounded window still starts where promoted latest starts without requiring a full observed_swaps scan.".to_string();
            let basis_explanation = "the current source bounded window start is proven from the bounded oldest retained observed_swaps row, and that current start matches promoted latest".to_string();
            return (
                RecentRawSourceWindowContractReasonClass::RecentRawSourceWindowCurrentStartMatchesPromotedStart,
                true,
                RecentRawSourceWindowContractBasis::MatchesPromotedStart,
                explanation,
                basis_explanation,
            );
        }

        if source_start_later_than_promoted == Some(true)
            && source_contract_currently_excludes_older_rows == Some(true)
            && source_prune_activity_recorded == Some(true)
        {
            let explanation = "current artifacts show that the cached recent_raw_journal_state matches the bounded oldest/newest observed_swaps probe, that current source covered_since is later than promoted latest on the same source path, and that the source journal records prune activity. This proves the current source-side bounded window has already excluded older rows that promoted latest still reflects, without a deep observed_swaps scan.".to_string();
            let basis_explanation = "the current source bounded window start is proven from the bounded oldest retained observed_swaps row plus recorded prune activity, and that oldest retained row now starts later than promoted latest".to_string();
            return (
                RecentRawSourceWindowContractReasonClass::RecentRawSourceWindowCurrentContractExcludesOlderRows,
                true,
                RecentRawSourceWindowContractBasis::CurrentSourceObservedSwapsWindowExcludesOlderRows,
                explanation,
                basis_explanation,
            );
        }

        if source_start_later_than_promoted == Some(true)
            && source_contract_currently_excludes_older_rows == Some(true)
            && promoted_reflects_older_still_promoted_window == Some(true)
        {
            let explanation = "current artifacts show that the cached recent_raw_journal_state matches the bounded oldest/newest observed_swaps probe and that the current source covered_since starts later than promoted latest on the same source path. This proves promoted latest still reflects an older still-promoted window while the current source window has already advanced to a later start, without a deep observed_swaps scan.".to_string();
            let basis_explanation = "the current source bounded window start is proven from the bounded oldest retained observed_swaps row, and promoted latest still exposes an older covered_since on that same source path".to_string();
            return (
                RecentRawSourceWindowContractReasonClass::RecentRawSourceWindowPromotedSurfaceReflectsOlderWindow,
                true,
                RecentRawSourceWindowContractBasis::PromotedStillReflectsOlderWindowWhileCurrentSourceStartsLater,
                explanation,
                basis_explanation,
            );
        }

        let explanation = format!(
            "recent_raw source window contract evidence remains unproven from current artifacts: source_start_matches_promoted={}, source_start_later_than_promoted={}, source_contract_currently_excludes_older_rows={}, promoted_reflects_older_still_promoted_window={}, source_prune_activity_recorded={}",
            source_start_matches_promoted_start
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            source_start_later_than_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            source_contract_currently_excludes_older_rows
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            promoted_reflects_older_still_promoted_window
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            source_prune_activity_recorded
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
        );
        let basis_explanation = "current artifacts do not reduce the current source window / promoted latest relation to one exact proven contract seam without inference".to_string();
        (
            RecentRawSourceWindowContractReasonClass::RecentRawSourceWindowCurrentAndPromotedContractRelationUnproven,
            false,
            RecentRawSourceWindowContractBasis::Unproven,
            explanation,
            basis_explanation,
        )
    }
}
