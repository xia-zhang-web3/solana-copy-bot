use super::*;

impl DiscoveryService {
    pub(crate) fn classify_recent_raw_staged_window_seeding(
        state: &RecentRawDiagnosticState,
        same_source_db_as_promoted: Option<bool>,
        staged_start_matches_promoted_start: Option<bool>,
        staged_start_matches_current_window_cutoff: Option<bool>,
        staged_start_matches_neither_promoted_nor_source: Option<bool>,
        staged_manifest_matches_sqlite_content: Option<bool>,
        sqlite_inspection_error: Option<&str>,
    ) -> (
        RecentRawStagedWindowSeedingReasonClass,
        bool,
        RecentRawStagedWindowSeedingBasis,
        String,
        String,
    ) {
        if state.promoted.manifest_error.is_some() || state.staged.manifest_error.is_some() {
            let explanation = format!(
                "recent_raw staged window current evidence is unproven because one or more snapshot surfaces are partial or invalid (promoted_error={}, staged_error={})",
                state.promoted.manifest_error.as_deref().unwrap_or("null"),
                state.staged.manifest_error.as_deref().unwrap_or("null"),
            );
            return (
                RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentEvidenceUnprovenDueToMissingEvidence,
                false,
                RecentRawStagedWindowSeedingBasis::Unproven,
                explanation.clone(),
                explanation,
            );
        }

        if !state.promoted_exists || !state.staged_exists || !state.source_state_available {
            let explanation = format!(
                "recent_raw staged window current evidence is unproven because promoted_exists={}, staged_exists={}, and source_state_available={}",
                state.promoted_exists, state.staged_exists, state.source_state_available
            );
            return (
                RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentEvidenceUnprovenDueToMissingEvidence,
                false,
                RecentRawStagedWindowSeedingBasis::Unproven,
                explanation.clone(),
                explanation,
            );
        }

        match same_source_db_as_promoted {
            Some(false) => {
                let explanation = "recent_raw staged window current evidence is unproven on this run because staged and promoted do not point at the same source_db_path".to_string();
                return (
                    RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentEvidenceUnprovenDueToMissingEvidence,
                    false,
                    RecentRawStagedWindowSeedingBasis::Unproven,
                    explanation.clone(),
                    explanation,
                );
            }
            None => {
                let explanation = "recent_raw staged window current evidence is unproven because the staged/promoted source_db_path values could not both be proven from current manifests".to_string();
                return (
                    RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentEvidenceUnprovenDueToMissingEvidence,
                    false,
                    RecentRawStagedWindowSeedingBasis::Unproven,
                    explanation.clone(),
                    explanation,
                );
            }
            Some(true) => {}
        }

        if staged_manifest_matches_sqlite_content == Some(false) {
            let explanation = "recent_raw staged window current evidence is unproven because the selected staged manifest and the current staged sqlite content disagree on covered_since, covered_through, or row_count".to_string();
            return (
                RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentEvidenceUnprovenDueToMissingEvidence,
                false,
                RecentRawStagedWindowSeedingBasis::Unproven,
                explanation.clone(),
                explanation,
            );
        }

        if staged_manifest_matches_sqlite_content.is_none() {
            let explanation = format!(
                "recent_raw staged window current evidence is unproven because the selected staged sqlite content could not be proven read-only from {}{}",
                state.staged_snapshot_path.display(),
                sqlite_inspection_error
                    .map(|error| format!(" ({error})"))
                    .unwrap_or_default()
            );
            return (
                RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentEvidenceUnprovenDueToMissingEvidence,
                false,
                RecentRawStagedWindowSeedingBasis::Unproven,
                explanation.clone(),
                explanation,
            );
        }

        if staged_start_matches_promoted_start == Some(true)
            && staged_start_matches_current_window_cutoff == Some(true)
        {
            let explanation = "current artifacts show that recent_raw staged covered_since matches both promoted latest and the current source covered_since on the same source_db_path. That proves only a current frontier equality across both surfaces, not the historical seeding input used when the staged artifact was formed.".to_string();
            let basis_explanation = "current artifacts prove only that staged covered_since equals both promoted latest and the current source covered_since".to_string();
            return (
                RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentStartMatchesBothPromotedAndSourceStart,
                true,
                RecentRawStagedWindowSeedingBasis::MatchesBothPromotedAndCurrentSourceStart,
                explanation,
                basis_explanation,
            );
        }

        if staged_start_matches_promoted_start == Some(true) {
            let explanation = "current artifacts show that recent_raw staged covered_since matches promoted latest while differing from the current source covered_since on the same source_db_path. That proves a current match to promoted latest, not the historical seeding input used when the staged artifact was formed.".to_string();
            let basis_explanation = "current artifacts prove only that staged covered_since equals promoted latest and differs from the current source covered_since".to_string();
            return (
                RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentStartMatchesPromotedStart,
                true,
                RecentRawStagedWindowSeedingBasis::MatchesPromotedStart,
                explanation,
                basis_explanation,
            );
        }

        if staged_start_matches_current_window_cutoff == Some(true) {
            let explanation = "current artifacts show that recent_raw staged covered_since matches the current source covered_since while differing from promoted latest on the same source_db_path. That proves a current match to the current source start, not the historical seeding input used when the staged artifact was formed.".to_string();
            let basis_explanation = "current artifacts prove only that staged covered_since equals the current source covered_since and differs from promoted latest".to_string();
            return (
                RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentStartMatchesSourceStart,
                true,
                RecentRawStagedWindowSeedingBasis::MatchesCurrentSourceStart,
                explanation,
                basis_explanation,
            );
        }

        if staged_start_matches_neither_promoted_nor_source == Some(true) {
            let explanation = "current artifacts show that recent_raw staged covered_since matches neither promoted latest nor the current source covered_since on the same source_db_path. That proves only that the current staged start differs from both currently observable start frontiers; it does not prove a historical internal checkpoint or any other historical seeding input.".to_string();
            let basis_explanation = "current artifacts prove only that staged covered_since differs from both promoted latest and the current source covered_since".to_string();
            return (
                RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentStartMatchesNeitherPromotedNorSource,
                true,
                RecentRawStagedWindowSeedingBasis::MatchesNeitherPromotedNorCurrentSourceStart,
                explanation,
                basis_explanation,
            );
        }

        let explanation = format!(
            "recent_raw staged window current evidence remains unproven from current artifacts: start_matches_promoted={}, start_matches_current_source={}, start_matches_neither_promoted_nor_source={}",
            staged_start_matches_promoted_start
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            staged_start_matches_current_window_cutoff
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            staged_start_matches_neither_promoted_nor_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
        );
        let basis_explanation =
            "current artifacts do not prove one complete staged-start relation without inference"
                .to_string();
        (
            RecentRawStagedWindowSeedingReasonClass::RecentRawStagedWindowCurrentEvidenceUnprovenDueToMissingEvidence,
            false,
            RecentRawStagedWindowSeedingBasis::Unproven,
            explanation,
            basis_explanation,
        )
    }
}
