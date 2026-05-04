impl DiscoveryService {
    pub fn explain_recent_raw_staged_regression_read_only(
        state_root: &Path,
    ) -> Result<RecentRawStagedRegressionDiagnostic> {
        let state = Self::load_recent_raw_diagnostic_state_read_only(state_root);
        let promoted_manifest = state.promoted.manifest.as_ref();
        let staged_manifest = state.staged.manifest.as_ref();
        let same_source_db_as_promoted = match (staged_manifest, promoted_manifest) {
            (Some(staged_manifest), Some(promoted_manifest)) => {
                Some(staged_manifest.source_db_path == promoted_manifest.source_db_path)
            }
            _ => None,
        };
        let cursor_relation_to_promoted = Self::recent_raw_lineage_relation_from_cursor(
            staged_manifest.and_then(|manifest| manifest.covered_through_cursor.as_ref()),
            promoted_manifest.and_then(|manifest| manifest.covered_through_cursor.as_ref()),
            same_source_db_as_promoted,
        );
        let row_count_relation_to_promoted =
            Self::recent_raw_lineage_relation_from_optional_row_count(
                staged_manifest.map(|manifest| manifest.row_count),
                promoted_manifest.map(|manifest| manifest.row_count),
                same_source_db_as_promoted,
            );
        let covered_since_relation_to_promoted =
            Self::recent_raw_lineage_relation_from_covered_since(
                staged_manifest.and_then(|manifest| manifest.covered_since.as_ref()),
                promoted_manifest.and_then(|manifest| manifest.covered_since.as_ref()),
                same_source_db_as_promoted,
            );
        let selected_staged_frontier_behind_promoted_before_comparison =
            match same_source_db_as_promoted {
                Some(true) => Some(
                    matches!(
                        cursor_relation_to_promoted,
                        RecentRawLineageRelation::Behind
                    ) || matches!(
                        row_count_relation_to_promoted,
                        RecentRawLineageRelation::Behind
                    ) || matches!(
                        covered_since_relation_to_promoted,
                        RecentRawLineageRelation::Behind
                    ),
                ),
                Some(false) | None => None,
            };
        let selected_staged_created_after_promoted = match (staged_manifest, promoted_manifest) {
            (Some(staged_manifest), Some(promoted_manifest)) => {
                Some(staged_manifest.created_at > promoted_manifest.created_at)
            }
            _ => None,
        };
        let selected_staged_completed_after_creation = staged_manifest.and_then(|manifest| {
            manifest
                .last_batch_completed_at
                .map(|last_batch_completed_at| last_batch_completed_at > manifest.created_at)
        });
        let parseable_candidate_created_at_max = state
            .staged_candidates
            .iter()
            .filter_map(|candidate| {
                candidate
                    .surface
                    .manifest
                    .as_ref()
                    .map(|manifest| manifest.created_at)
            })
            .max();
        let selected_staged_is_latest_candidate = match (
            staged_manifest.map(|manifest| manifest.created_at),
            parseable_candidate_created_at_max,
        ) {
            (Some(selected_created_at), Some(candidate_created_at_max)) => {
                Some(selected_created_at >= candidate_created_at_max)
            }
            _ => None,
        };
        let staged_selection_reason = Self::recent_raw_staged_selection_reason(
            &state.staged_snapshot_path,
            &state.staged_metadata_path,
            state.staged_candidates.len(),
            selected_staged_is_latest_candidate,
            state.staged_exists,
        );
        let (reason_class, observed, explanation) = Self::classify_recent_raw_staged_regression(
            &state,
            same_source_db_as_promoted,
            selected_staged_frontier_behind_promoted_before_comparison,
            selected_staged_created_after_promoted,
            selected_staged_is_latest_candidate,
        );

        Ok(RecentRawStagedRegressionDiagnostic {
            recent_raw_snapshot_dir: state.snapshot_dir.display().to_string(),
            recent_raw_staged_regression_observed: observed,
            recent_raw_staged_regression_reason_class: reason_class,
            recent_raw_staged_regression_explanation: explanation,
            recent_raw_promoted_source_db_path: promoted_manifest
                .map(|manifest| manifest.source_db_path.clone()),
            recent_raw_promoted_snapshot_path: state.promoted_snapshot_path.display().to_string(),
            recent_raw_promoted_metadata_path: state.promoted_metadata_path.display().to_string(),
            recent_raw_promoted_created_at: promoted_manifest.map(|manifest| manifest.created_at),
            recent_raw_promoted_covered_since: promoted_manifest
                .and_then(|manifest| manifest.covered_since),
            recent_raw_promoted_covered_through: promoted_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_promoted_row_count: promoted_manifest.map(|manifest| manifest.row_count),
            recent_raw_promoted_last_batch_completed_at: promoted_manifest
                .and_then(|manifest| manifest.last_batch_completed_at),
            recent_raw_staged_source_db_path: staged_manifest
                .map(|manifest| manifest.source_db_path.clone()),
            recent_raw_staged_snapshot_path: state.staged_snapshot_path.display().to_string(),
            recent_raw_staged_metadata_path: state.staged_metadata_path.display().to_string(),
            recent_raw_staged_created_at: staged_manifest.map(|manifest| manifest.created_at),
            recent_raw_staged_covered_since: staged_manifest
                .and_then(|manifest| manifest.covered_since),
            recent_raw_staged_covered_through: staged_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_staged_row_count: staged_manifest.map(|manifest| manifest.row_count),
            recent_raw_staged_last_batch_completed_at: staged_manifest
                .and_then(|manifest| manifest.last_batch_completed_at),
            recent_raw_staged_candidate_count: state.staged_candidates.len(),
            recent_raw_multiple_staged_candidates_present: state.staged_candidates.len() > 1,
            recent_raw_staged_candidate_snapshot_paths: state
                .staged_candidates
                .iter()
                .map(|candidate| candidate.snapshot_path.display().to_string())
                .collect(),
            recent_raw_staged_candidate_metadata_paths: state
                .staged_candidates
                .iter()
                .map(|candidate| candidate.metadata_path.display().to_string())
                .collect(),
            recent_raw_selected_staged_is_latest_candidate: selected_staged_is_latest_candidate,
            recent_raw_selected_staged_created_after_promoted:
                selected_staged_created_after_promoted,
            recent_raw_selected_staged_frontier_behind_promoted_before_comparison:
                selected_staged_frontier_behind_promoted_before_comparison,
            recent_raw_staged_selection_reason: staged_selection_reason,
            recent_raw_selected_staged_completed_after_creation:
                selected_staged_completed_after_creation,
            recent_raw_staged_same_source_db_as_promoted: same_source_db_as_promoted,
            recent_raw_selected_staged_cursor_relation_to_promoted: cursor_relation_to_promoted,
            recent_raw_selected_staged_row_count_relation_to_promoted:
                row_count_relation_to_promoted,
            recent_raw_selected_staged_covered_since_relation_to_promoted:
                covered_since_relation_to_promoted,
        })
    }

    pub fn explain_recent_raw_staged_birth_read_only(
        state_root: &Path,
    ) -> Result<RecentRawStagedBirthDiagnostic> {
        let state = Self::load_recent_raw_diagnostic_state_read_only(state_root);
        let promoted_manifest = state.promoted.manifest.as_ref();
        let staged_manifest = state.staged.manifest.as_ref();
        let same_source_db_as_promoted = match (staged_manifest, promoted_manifest) {
            (Some(staged_manifest), Some(promoted_manifest)) => {
                Some(staged_manifest.source_db_path == promoted_manifest.source_db_path)
            }
            _ => None,
        };
        let created_after_promoted = match (staged_manifest, promoted_manifest) {
            (Some(staged_manifest), Some(promoted_manifest)) => {
                Some(staged_manifest.created_at > promoted_manifest.created_at)
            }
            _ => None,
        };
        let covered_since_relation_to_promoted =
            Self::recent_raw_lineage_relation_from_covered_since(
                staged_manifest.and_then(|manifest| manifest.covered_since.as_ref()),
                promoted_manifest.and_then(|manifest| manifest.covered_since.as_ref()),
                same_source_db_as_promoted,
            );
        let covered_through_relation_to_promoted = Self::recent_raw_lineage_relation_from_cursor(
            staged_manifest.and_then(|manifest| manifest.covered_through_cursor.as_ref()),
            promoted_manifest.and_then(|manifest| manifest.covered_through_cursor.as_ref()),
            same_source_db_as_promoted,
        );
        let row_count_relation_to_promoted =
            Self::recent_raw_lineage_relation_from_optional_row_count(
                staged_manifest.map(|manifest| manifest.row_count),
                promoted_manifest.map(|manifest| manifest.row_count),
                same_source_db_as_promoted,
            );
        let window_later_start_than_promoted = match same_source_db_as_promoted {
            Some(true) => Some(matches!(
                covered_since_relation_to_promoted,
                RecentRawLineageRelation::Behind
            )),
            Some(false) | None => None,
        };
        let window_narrower_or_older_than_promoted = match same_source_db_as_promoted {
            Some(true) => Some(
                matches!(
                    covered_since_relation_to_promoted,
                    RecentRawLineageRelation::Behind
                ) || matches!(
                    covered_through_relation_to_promoted,
                    RecentRawLineageRelation::Behind
                ) || matches!(
                    row_count_relation_to_promoted,
                    RecentRawLineageRelation::Behind
                ),
            ),
            Some(false) | None => None,
        };
        let source_outruns_both = match (state.source_outruns_staged, state.source_outruns_promoted)
        {
            (Some(staged), Some(promoted)) => Some(staged && promoted),
            _ => None,
        };

        let sqlite_content =
            Self::read_recent_raw_snapshot_sqlite_content_read_only(&state.staged_snapshot_path);
        let sqlite_state = sqlite_content.state.as_ref();
        let manifest_matches_sqlite_content = match (staged_manifest, sqlite_state) {
            (Some(manifest), Some(sqlite_state)) => Some(
                manifest.row_count == sqlite_state.row_count
                    && manifest.covered_since == sqlite_state.covered_since
                    && Self::recent_raw_optional_cursor_equal(
                        manifest.covered_through_cursor.as_ref(),
                        sqlite_state.covered_through_cursor.as_ref(),
                    ),
            ),
            _ => None,
        };
        let manifest_sqlite_match_unproven = manifest_matches_sqlite_content.is_none();
        let (reason_class, observed, explanation) = Self::classify_recent_raw_staged_birth(
            &state,
            same_source_db_as_promoted,
            created_after_promoted,
            covered_since_relation_to_promoted,
            covered_through_relation_to_promoted,
            row_count_relation_to_promoted,
            manifest_matches_sqlite_content,
            sqlite_content.error.as_deref(),
        );

        Ok(RecentRawStagedBirthDiagnostic {
            recent_raw_snapshot_dir: state.snapshot_dir.display().to_string(),
            recent_raw_staged_birth_observed: observed,
            recent_raw_staged_birth_proven_from_current_artifacts: false,
            recent_raw_staged_birth_reason_class: reason_class,
            recent_raw_staged_birth_explanation: explanation,
            recent_raw_staged_birth_snapshot_path: state.staged_snapshot_path.display().to_string(),
            recent_raw_staged_birth_metadata_path: state.staged_metadata_path.display().to_string(),
            recent_raw_staged_birth_created_at: staged_manifest.map(|manifest| manifest.created_at),
            recent_raw_staged_birth_covered_since: staged_manifest
                .and_then(|manifest| manifest.covered_since),
            recent_raw_staged_birth_covered_through: staged_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_staged_birth_row_count: staged_manifest.map(|manifest| manifest.row_count),
            recent_raw_promoted_created_at: promoted_manifest.map(|manifest| manifest.created_at),
            recent_raw_promoted_covered_since: promoted_manifest
                .and_then(|manifest| manifest.covered_since),
            recent_raw_promoted_covered_through: promoted_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_promoted_row_count: promoted_manifest.map(|manifest| manifest.row_count),
            recent_raw_staged_birth_created_after_promoted: created_after_promoted,
            recent_raw_staged_birth_covered_since_relation_to_promoted:
                covered_since_relation_to_promoted,
            recent_raw_staged_birth_covered_through_relation_to_promoted:
                covered_through_relation_to_promoted,
            recent_raw_staged_birth_row_count_relation_to_promoted: row_count_relation_to_promoted,
            recent_raw_staged_birth_window_later_start_than_promoted:
                window_later_start_than_promoted,
            recent_raw_staged_birth_window_narrower_or_older_than_promoted:
                window_narrower_or_older_than_promoted,
            recent_raw_staged_birth_same_source_db_as_promoted: same_source_db_as_promoted,
            recent_raw_staged_birth_source_outruns_both: source_outruns_both,
            recent_raw_staged_birth_manifest_matches_sqlite_content:
                manifest_matches_sqlite_content,
            recent_raw_staged_birth_manifest_sqlite_match_unproven: manifest_sqlite_match_unproven,
            recent_raw_staged_birth_sqlite_covered_since: sqlite_state
                .and_then(|state| state.covered_since),
            recent_raw_staged_birth_sqlite_covered_through: sqlite_state
                .and_then(|state| state.covered_through_cursor.clone()),
            recent_raw_staged_birth_sqlite_row_count: sqlite_state.map(|state| state.row_count),
            recent_raw_staged_birth_sqlite_last_batch_completed_at: sqlite_state
                .and_then(|state| state.last_batch_completed_at),
            recent_raw_staged_birth_sqlite_inspection_error: sqlite_content.error,
        })
    }
}
