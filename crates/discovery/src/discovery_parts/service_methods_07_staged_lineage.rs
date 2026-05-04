impl DiscoveryService {
    fn classify_recent_raw_staged_lineage(
        state: &RecentRawDiagnosticState,
        same_source_db_as_promoted: Option<bool>,
        cursor_relation_to_promoted: RecentRawLineageRelation,
        row_count_relation_to_promoted: RecentRawLineageRelation,
        covered_since_relation_to_promoted: RecentRawLineageRelation,
    ) -> (RecentRawStagedLineageReasonClass, bool, String) {
        if state.promoted.manifest_error.is_some() || state.staged.manifest_error.is_some() {
            return (
                RecentRawStagedLineageReasonClass::RecentRawStagedLineageUnprovenDueToMissingEvidence,
                false,
                format!(
                    "recent_raw staged lineage is unproven because one or more snapshot surfaces are partial or invalid (promoted_error={}, staged_error={})",
                    state.promoted.manifest_error.as_deref().unwrap_or("null"),
                    state.staged.manifest_error.as_deref().unwrap_or("null")
                ),
            );
        }

        if !state.promoted_exists || !state.staged_exists {
            return (
                RecentRawStagedLineageReasonClass::RecentRawStagedLineageUnprovenDueToMissingEvidence,
                false,
                format!(
                    "recent_raw staged lineage is unproven because promoted_exists={} and staged_exists={}",
                    state.promoted_exists, state.staged_exists
                ),
            );
        }

        match same_source_db_as_promoted {
            Some(false) => {
                return (
                    RecentRawStagedLineageReasonClass::RecentRawStagedLineagePointsToDifferentSource,
                    true,
                    "recent_raw staged surface points at a different source_db_path than the currently promoted latest surface, so their lineages are not directly monotonic".to_string(),
                )
            }
            None => {
                return (
                    RecentRawStagedLineageReasonClass::RecentRawStagedLineageUnprovenDueToMissingEvidence,
                    false,
                    "recent_raw staged lineage is unproven because the promoted and staged source_db_path values could not both be proven from current manifests".to_string(),
                )
            }
            Some(true) => {}
        }

        let regressed = matches!(
            cursor_relation_to_promoted,
            RecentRawLineageRelation::Behind
        ) || matches!(
            row_count_relation_to_promoted,
            RecentRawLineageRelation::Behind
        ) || matches!(
            covered_since_relation_to_promoted,
            RecentRawLineageRelation::Behind
        );
        if regressed {
            return (
                RecentRawStagedLineageReasonClass::RecentRawStagedLineageRegressedRelativeToPromoted,
                true,
                "recent_raw staged surface points at the same source_db_path as promoted latest, but its current frontier is older by one or more monotonic dimensions (covered_since, covered_through, or row_count), so the staged lineage has regressed relative to promoted".to_string(),
            );
        }

        (
            RecentRawStagedLineageReasonClass::RecentRawStagedLineageMonotonicButIncomplete,
            true,
            "recent_raw staged surface points at the same source_db_path as promoted latest and does not regress on covered_since, covered_through, or row_count, so the staged lineage is monotonic even though it may still be incomplete against the current source".to_string(),
        )
    }

    fn classify_recent_raw_staged_regression(
        state: &RecentRawDiagnosticState,
        same_source_db_as_promoted: Option<bool>,
        selected_staged_frontier_behind_promoted_before_comparison: Option<bool>,
        selected_staged_created_after_promoted: Option<bool>,
        selected_staged_is_latest_candidate: Option<bool>,
    ) -> (RecentRawStagedRegressionReasonClass, bool, String) {
        if state.promoted.manifest_error.is_some() || state.staged.manifest_error.is_some() {
            return (
                RecentRawStagedRegressionReasonClass::RecentRawStagedRegressionUnprovenDueToMissingEvidence,
                false,
                format!(
                    "recent_raw staged regression mechanism is unproven because one or more snapshot surfaces are partial or invalid (promoted_error={}, staged_error={})",
                    state
                        .promoted
                        .manifest_error
                        .as_deref()
                        .unwrap_or("null"),
                    state.staged.manifest_error.as_deref().unwrap_or("null"),
                ),
            );
        }

        match same_source_db_as_promoted {
            Some(false) => {
                return (
                    RecentRawStagedRegressionReasonClass::RecentRawStagedRegressionUnprovenDueToMissingEvidence,
                    false,
                    "recent_raw staged regression mechanism is unproven on this run because the staged and promoted artifacts do not point at the same source_db_path".to_string(),
                )
            }
            None => {
                return (
                    RecentRawStagedRegressionReasonClass::RecentRawStagedRegressionUnprovenDueToMissingEvidence,
                    false,
                    "recent_raw staged regression mechanism is unproven because the staged/promoted source_db_path values could not both be proven from current manifests".to_string(),
                )
            }
            Some(true) => {}
        }

        let Some(frontier_behind) = selected_staged_frontier_behind_promoted_before_comparison
        else {
            return (
                RecentRawStagedRegressionReasonClass::RecentRawStagedRegressionUnprovenDueToMissingEvidence,
                false,
                "recent_raw staged regression mechanism is unproven because the selected staged artifact could not be compared directly against promoted latest on the same source lineage".to_string(),
            );
        };

        if !frontier_behind {
            return (
                RecentRawStagedRegressionReasonClass::RecentRawStagedRegressionNoCurrentSameSourceRegressionObserved,
                true,
                "recent_raw staged artifact is selected from the same source_db_path as promoted latest and its own frontier is not behind promoted before any promotion comparison logic runs, so no current same-source staged regression is observed".to_string(),
            );
        }

        if state.staged_candidates.len() > 1 && selected_staged_is_latest_candidate == Some(false) {
            return (
                RecentRawStagedRegressionReasonClass::RecentRawStagedRegressionMultipleCandidatesWrongSelection,
                true,
                format!(
                    "recent_raw staged regression is explained by artifact selection: diagnostics select the fixed staged artifact path {}, but candidate scan found {} staged candidates and the selected artifact is not the newest parseable candidate by created_at",
                    state.staged_snapshot_path.display(),
                    state.staged_candidates.len()
                ),
            );
        }

        if selected_staged_created_after_promoted == Some(true) {
            return (
                RecentRawStagedRegressionReasonClass::RecentRawStagedRegressionArtifactItselfAlreadyBehind,
                true,
                "recent_raw staged regression is explained by the selected staged artifact itself: it was created after promoted latest, but its own covered_since / covered_through / row_count frontier is already behind promoted before any promotion comparison logic runs".to_string(),
            );
        }

        (
            RecentRawStagedRegressionReasonClass::RecentRawStagedRegressionSelectedOlderStagedArtifact,
            true,
            "recent_raw staged regression is explained by the currently selected fixed staged artifact already being behind promoted latest on the same source lineage before any promotion comparison logic runs".to_string(),
        )
    }

    fn classify_recent_raw_staged_birth(
        state: &RecentRawDiagnosticState,
        same_source_db_as_promoted: Option<bool>,
        created_after_promoted: Option<bool>,
        covered_since_relation_to_promoted: RecentRawLineageRelation,
        covered_through_relation_to_promoted: RecentRawLineageRelation,
        row_count_relation_to_promoted: RecentRawLineageRelation,
        manifest_matches_sqlite_content: Option<bool>,
        sqlite_inspection_error: Option<&str>,
    ) -> (RecentRawStagedBirthReasonClass, bool, String) {
        if state.promoted.manifest_error.is_some() || state.staged.manifest_error.is_some() {
            return (
                RecentRawStagedBirthReasonClass::RecentRawStagedCurrentArtifactUnprovenDueToMissingEvidence,
                false,
                format!(
                    "recent_raw staged current-artifact evidence is unproven because one or more snapshot surfaces are partial or invalid (promoted_error={}, staged_error={})",
                    state.promoted.manifest_error.as_deref().unwrap_or("null"),
                    state.staged.manifest_error.as_deref().unwrap_or("null"),
                ),
            );
        }

        match same_source_db_as_promoted {
            Some(false) => {
                return (
                    RecentRawStagedBirthReasonClass::RecentRawStagedCurrentArtifactUnprovenDueToMissingEvidence,
                    false,
                    "recent_raw staged current-artifact evidence is unproven on this run because staged and promoted do not point at the same source_db_path".to_string(),
                )
            }
            None => {
                return (
                    RecentRawStagedBirthReasonClass::RecentRawStagedCurrentArtifactUnprovenDueToMissingEvidence,
                    false,
                    "recent_raw staged current-artifact evidence is unproven because the staged/promoted source_db_path values could not both be proven from current manifests".to_string(),
                )
            }
            Some(true) => {}
        }

        let birth_behind = matches!(
            covered_since_relation_to_promoted,
            RecentRawLineageRelation::Behind
        ) || matches!(
            covered_through_relation_to_promoted,
            RecentRawLineageRelation::Behind
        ) || matches!(
            row_count_relation_to_promoted,
            RecentRawLineageRelation::Behind
        );

        if manifest_matches_sqlite_content == Some(false) {
            return (
                RecentRawStagedBirthReasonClass::RecentRawStagedCurrentArtifactManifestSqliteMismatch,
                true,
                "recent_raw staged current-artifact evidence shows the selected staged manifest and the current staged sqlite content disagree on covered_since, covered_through, or row_count. This proves a current artifact mismatch, not a creation-time frontier.".to_string(),
            );
        }

        if manifest_matches_sqlite_content.is_none() {
            return (
                RecentRawStagedBirthReasonClass::RecentRawStagedCurrentArtifactUnprovenDueToMissingEvidence,
                false,
                format!(
                    "recent_raw staged current-artifact evidence is unproven because the selected staged sqlite content could not be proven read-only from {}{}",
                    state.staged_snapshot_path.display(),
                    sqlite_inspection_error
                        .map(|error| format!(" ({error})"))
                        .unwrap_or_default()
                ),
            );
        }

        if !birth_behind {
            return (
                RecentRawStagedBirthReasonClass::RecentRawStagedCurrentArtifactNotCurrentlyBehindPromoted,
                true,
                "recent_raw staged current-artifact facts are internally consistent and the selected staged artifact is not currently behind promoted latest on covered_since, covered_through, or row_count. Current artifacts do not prove what frontier existed at creation time.".to_string(),
            );
        }

        if matches!(
            covered_since_relation_to_promoted,
            RecentRawLineageRelation::Behind
        ) {
            return (
                RecentRawStagedBirthReasonClass::RecentRawStagedCurrentArtifactLaterStartWindowObserved,
                true,
                format!(
                    "recent_raw staged current-artifact evidence shows a later/narrower start window than promoted latest: the selected staged artifact was created {} promoted latest on the same source_db_path, but its current covered_since starts later and it is currently behind promoted on covered_through or row_count. Current artifacts do not prove whether that later-start window was already present at creation time.",
                    created_after_promoted
                        .map(|value| if value { "after" } else { "before or at the same time as" })
                        .unwrap_or("relative to")
                ),
            );
        }

        (
            RecentRawStagedBirthReasonClass::RecentRawStagedCurrentArtifactManifestAndSqliteContentAgreeButBehind,
            true,
            "recent_raw staged manifest and current staged sqlite content agree on covered_since, covered_through, and row_count, but the selected staged artifact is currently behind promoted latest on the same source_db_path. Current artifacts do not prove whether that behind frontier was already present at creation time.".to_string(),
        )
    }
}
