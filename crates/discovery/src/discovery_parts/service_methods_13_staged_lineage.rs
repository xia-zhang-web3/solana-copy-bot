impl DiscoveryService {
    pub fn explain_recent_raw_staged_lineage_read_only(
        state_root: &Path,
    ) -> Result<RecentRawStagedLineageDiagnostic> {
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
        let cursor_ts_relation_to_promoted = Self::recent_raw_lineage_relation_from_ord(
            staged_manifest
                .and_then(|manifest| manifest.covered_through_cursor.as_ref())
                .map(|cursor| cursor.ts_utc),
            promoted_manifest
                .and_then(|manifest| manifest.covered_through_cursor.as_ref())
                .map(|cursor| cursor.ts_utc),
            same_source_db_as_promoted,
        );
        let cursor_slot_relation_to_promoted = Self::recent_raw_lineage_relation_from_ord(
            staged_manifest
                .and_then(|manifest| manifest.covered_through_cursor.as_ref())
                .map(|cursor| cursor.slot),
            promoted_manifest
                .and_then(|manifest| manifest.covered_through_cursor.as_ref())
                .map(|cursor| cursor.slot),
            same_source_db_as_promoted,
        );
        let cursor_signature_equal_to_promoted = match same_source_db_as_promoted {
            Some(true) => match (
                staged_manifest.and_then(|manifest| manifest.covered_through_cursor.as_ref()),
                promoted_manifest.and_then(|manifest| manifest.covered_through_cursor.as_ref()),
            ) {
                (Some(staged_cursor), Some(promoted_cursor)) => {
                    Some(staged_cursor.signature == promoted_cursor.signature)
                }
                _ => None,
            },
            Some(false) | None => None,
        };
        let cursor_relation_explanation = Self::recent_raw_cursor_relation_explanation(
            same_source_db_as_promoted,
            cursor_relation_to_promoted,
            cursor_ts_relation_to_promoted,
            cursor_slot_relation_to_promoted,
            cursor_signature_equal_to_promoted,
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
        let staged_monotonic_relative_to_promoted = match same_source_db_as_promoted {
            Some(true) => Some(
                !matches!(
                    cursor_relation_to_promoted,
                    RecentRawLineageRelation::Behind
                ) && !matches!(
                    row_count_relation_to_promoted,
                    RecentRawLineageRelation::Behind
                ) && !matches!(
                    covered_since_relation_to_promoted,
                    RecentRawLineageRelation::Behind
                ),
            ),
            Some(false) | None => None,
        };
        let staged_regressed_relative_to_promoted =
            staged_monotonic_relative_to_promoted.map(|monotonic| !monotonic);
        let source_outruns_both = match (state.source_outruns_staged, state.source_outruns_promoted)
        {
            (Some(staged), Some(promoted)) => Some(staged && promoted),
            _ => None,
        };
        let staged_closer_to_source_than_promoted = if same_source_db_as_promoted == Some(true) {
            Self::recent_raw_candidate_closer_to_source_than_reference(
                state.staged_vs_promoted_relation,
                state.source_outruns_staged,
                state.source_outruns_promoted,
            )
        } else {
            None
        };
        let (reason_class, observed, explanation) = Self::classify_recent_raw_staged_lineage(
            &state,
            same_source_db_as_promoted,
            cursor_relation_to_promoted,
            row_count_relation_to_promoted,
            covered_since_relation_to_promoted,
        );

        Ok(RecentRawStagedLineageDiagnostic {
            recent_raw_snapshot_dir: state.snapshot_dir.display().to_string(),
            recent_raw_staged_lineage_observed: observed,
            recent_raw_staged_lineage_reason_class: reason_class,
            recent_raw_staged_lineage_explanation: explanation,
            recent_raw_promoted_source_db_path: promoted_manifest
                .map(|manifest| manifest.source_db_path.clone()),
            recent_raw_promoted_created_at: promoted_manifest
                .map(|manifest| manifest.created_at.clone()),
            recent_raw_promoted_covered_since: promoted_manifest
                .and_then(|manifest| manifest.covered_since.clone()),
            recent_raw_promoted_covered_through: promoted_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_promoted_row_count: promoted_manifest.map(|manifest| manifest.row_count),
            recent_raw_promoted_last_batch_completed_at: promoted_manifest
                .and_then(|manifest| manifest.last_batch_completed_at.clone()),
            recent_raw_staged_source_db_path: staged_manifest
                .map(|manifest| manifest.source_db_path.clone()),
            recent_raw_staged_created_at: staged_manifest
                .map(|manifest| manifest.created_at.clone()),
            recent_raw_staged_covered_since: staged_manifest
                .and_then(|manifest| manifest.covered_since.clone()),
            recent_raw_staged_covered_through: staged_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_staged_row_count: staged_manifest.map(|manifest| manifest.row_count),
            recent_raw_staged_last_batch_completed_at: staged_manifest
                .and_then(|manifest| manifest.last_batch_completed_at.clone()),
            recent_raw_source_covered_through: state
                .source_state
                .as_ref()
                .and_then(|source| source.covered_through_cursor.clone()),
            recent_raw_source_row_count: state.source_state.as_ref().map(|source| source.row_count),
            recent_raw_source_outruns_staged: state.source_outruns_staged,
            recent_raw_source_outruns_promoted: state.source_outruns_promoted,
            recent_raw_source_outruns_both: source_outruns_both,
            recent_raw_staged_same_source_db_as_promoted: same_source_db_as_promoted,
            recent_raw_staged_cursor_relation_basis:
                RecentRawCursorRelationBasis::DirectCoveredThroughCursorComparison,
            recent_raw_staged_cursor_relation_explanation: cursor_relation_explanation,
            recent_raw_staged_cursor_relation_to_promoted: cursor_relation_to_promoted,
            recent_raw_staged_cursor_ts_relation_to_promoted: cursor_ts_relation_to_promoted,
            recent_raw_staged_cursor_slot_relation_to_promoted: cursor_slot_relation_to_promoted,
            recent_raw_staged_cursor_signature_equal_to_promoted:
                cursor_signature_equal_to_promoted,
            recent_raw_staged_row_count_relation_to_promoted: row_count_relation_to_promoted,
            recent_raw_staged_covered_since_relation_to_promoted:
                covered_since_relation_to_promoted,
            recent_raw_staged_monotonic_relative_to_promoted: staged_monotonic_relative_to_promoted,
            recent_raw_staged_regressed_relative_to_promoted: staged_regressed_relative_to_promoted,
            recent_raw_staged_closer_to_source_than_promoted: staged_closer_to_source_than_promoted,
        })
    }
}
