use super::*;

impl DiscoveryService {
    pub fn explain_recent_raw_replacement_progress_contract_read_only(
        state_root: &Path,
    ) -> Result<RecentRawReplacementProgressContractDiagnostic> {
        let state = Self::load_recent_raw_diagnostic_state_read_only(state_root);
        let staged_manifest = state.staged.manifest.as_ref();
        let replacement_promotion =
            Self::explain_recent_raw_replacement_promotion_contract_read_only(state_root)?;

        let sqlite_content =
            Self::read_recent_raw_snapshot_sqlite_content_read_only(&state.staged_snapshot_path);
        let sqlite_state = sqlite_content.state.as_ref();
        let replacement_manifest_matches_sqlite_content = match (staged_manifest, sqlite_state) {
            (Some(manifest), Some(sqlite_state)) => Some(
                manifest.row_count == sqlite_state.row_count
                    && manifest.covered_since == sqlite_state.covered_since
                    && Self::recent_raw_optional_cursor_equal(
                        manifest.covered_through_cursor.as_ref(),
                        sqlite_state.covered_through_cursor.as_ref(),
                    )
                    && manifest.last_batch_completed_at == sqlite_state.last_batch_completed_at,
            ),
            _ => None,
        };
        let replacement_manifest_sqlite_match_unproven =
            replacement_manifest_matches_sqlite_content.is_none();

        let previous_candidate = staged_manifest.and_then(|staged_manifest| {
            state
                .staged_candidates
                .iter()
                .filter(|candidate| candidate.snapshot_path != state.staged_snapshot_path)
                .filter_map(|candidate| {
                    candidate
                        .surface
                        .manifest
                        .as_ref()
                        .map(|manifest| (candidate, manifest))
                })
                .filter(|(_, manifest)| manifest.source_db_path == staged_manifest.source_db_path)
                .max_by(|(_, left), (_, right)| {
                    left.created_at
                        .cmp(&right.created_at)
                        .then_with(|| {
                            left.last_batch_completed_at
                                .unwrap_or(left.created_at)
                                .cmp(&right.last_batch_completed_at.unwrap_or(right.created_at))
                        })
                        .then_with(|| left.row_count.cmp(&right.row_count))
                })
        });

        let current_completed_after_creation = staged_manifest.and_then(|manifest| {
            manifest
                .last_batch_completed_at
                .map(|last_batch_completed_at| last_batch_completed_at > manifest.created_at)
        });

        let (
            same_identity_as_previous_attempt,
            row_count_advanced,
            covered_through_advanced,
            last_batch_completed_at_advanced,
            candidate_advancing,
            candidate_stalled,
            candidate_reset_or_recreated,
        ) = match (staged_manifest, previous_candidate.as_ref()) {
            (Some(staged_manifest), Some((_, previous_manifest))) => {
                let same_identity =
                    Some(staged_manifest.created_at == previous_manifest.created_at);
                let row_count_advanced = same_identity.and_then(|same_identity| {
                    same_identity.then_some(staged_manifest.row_count > previous_manifest.row_count)
                });
                let covered_through_advanced = same_identity.and_then(|same_identity| {
                    same_identity.then_some(
                        Self::recent_raw_lineage_relation_from_cursor(
                            staged_manifest.covered_through_cursor.as_ref(),
                            previous_manifest.covered_through_cursor.as_ref(),
                            Some(true),
                        ) == RecentRawLineageRelation::Ahead,
                    )
                });
                let last_batch_completed_at_advanced = same_identity.and_then(|same_identity| {
                    same_identity.then_some(
                        staged_manifest.last_batch_completed_at
                            > previous_manifest.last_batch_completed_at,
                    )
                });
                let candidate_advancing = match (
                    row_count_advanced,
                    covered_through_advanced,
                    last_batch_completed_at_advanced,
                ) {
                    (
                        Some(row_count_advanced),
                        Some(covered_through_advanced),
                        Some(last_batch_completed_at_advanced),
                    ) => Some(
                        row_count_advanced
                            || covered_through_advanced
                            || last_batch_completed_at_advanced,
                    ),
                    _ => None,
                };
                let candidate_stalled = match (
                    same_identity,
                    row_count_advanced,
                    covered_through_advanced,
                    last_batch_completed_at_advanced,
                ) {
                    (Some(true), Some(false), Some(false), Some(false)) => Some(true),
                    (Some(true), Some(_), Some(_), Some(_)) => Some(false),
                    _ => None,
                };
                let candidate_reset_or_recreated = match staged_manifest
                    .created_at
                    .cmp(&previous_manifest.created_at)
                {
                    Ordering::Greater => Some(true),
                    Ordering::Equal => Some(false),
                    Ordering::Less => None,
                };
                (
                    same_identity,
                    row_count_advanced,
                    covered_through_advanced,
                    last_batch_completed_at_advanced,
                    candidate_advancing,
                    candidate_stalled,
                    candidate_reset_or_recreated,
                )
            }
            (Some(_), None) => (None, None, None, None, None, None, None),
            _ => (None, None, None, None, None, None, None),
        };

        let (reason_class, observed, explanation) =
            Self::classify_recent_raw_replacement_progress_contract(
                state.staged.manifest_error.as_deref(),
                replacement_promotion.recent_raw_replacement_candidate_exists,
                replacement_promotion
                    .recent_raw_replacement_candidate_complete_against_current_source,
                replacement_promotion.recent_raw_replacement_promotion_reason_class,
                replacement_manifest_matches_sqlite_content,
                previous_candidate.is_some(),
                current_completed_after_creation,
                same_identity_as_previous_attempt,
                row_count_advanced,
                covered_through_advanced,
                last_batch_completed_at_advanced,
                candidate_advancing,
                candidate_stalled,
                candidate_reset_or_recreated,
            );

        Ok(RecentRawReplacementProgressContractDiagnostic {
            recent_raw_snapshot_dir: state.snapshot_dir.display().to_string(),
            recent_raw_replacement_progress_observed: observed,
            recent_raw_replacement_progress_reason_class: reason_class,
            recent_raw_replacement_progress_explanation: explanation,
            recent_raw_replacement_candidate_exists: replacement_promotion
                .recent_raw_replacement_candidate_exists,
            recent_raw_replacement_candidate_created_at: staged_manifest
                .map(|manifest| manifest.created_at),
            recent_raw_replacement_candidate_row_count: staged_manifest
                .map(|manifest| manifest.row_count),
            recent_raw_replacement_candidate_covered_through: staged_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_replacement_candidate_last_batch_completed_at: staged_manifest
                .and_then(|manifest| manifest.last_batch_completed_at),
            recent_raw_replacement_candidate_completed_after_creation:
                current_completed_after_creation,
            recent_raw_replacement_candidate_same_identity_as_previous_attempt:
                same_identity_as_previous_attempt,
            recent_raw_replacement_candidate_row_count_advanced: row_count_advanced,
            recent_raw_replacement_candidate_covered_through_advanced: covered_through_advanced,
            recent_raw_replacement_candidate_last_batch_completed_at_advanced:
                last_batch_completed_at_advanced,
            recent_raw_replacement_candidate_advancing: candidate_advancing,
            recent_raw_replacement_candidate_stalled: candidate_stalled,
            recent_raw_replacement_candidate_reset_or_recreated: candidate_reset_or_recreated,
            recent_raw_replacement_previous_candidate_exists: previous_candidate.is_some(),
            recent_raw_replacement_previous_candidate_snapshot_path: previous_candidate
                .as_ref()
                .map(|(candidate, _)| candidate.snapshot_path.display().to_string()),
            recent_raw_replacement_previous_candidate_metadata_path: previous_candidate
                .as_ref()
                .map(|(candidate, _)| candidate.metadata_path.display().to_string()),
            recent_raw_replacement_previous_candidate_created_at: previous_candidate
                .as_ref()
                .map(|(_, manifest)| manifest.created_at),
            recent_raw_replacement_previous_candidate_row_count: previous_candidate
                .as_ref()
                .map(|(_, manifest)| manifest.row_count),
            recent_raw_replacement_previous_candidate_covered_through: previous_candidate
                .as_ref()
                .and_then(|(_, manifest)| manifest.covered_through_cursor.clone()),
            recent_raw_replacement_previous_candidate_last_batch_completed_at: previous_candidate
                .as_ref()
                .and_then(|(_, manifest)| manifest.last_batch_completed_at),
            recent_raw_replacement_manifest_matches_sqlite_content:
                replacement_manifest_matches_sqlite_content,
            recent_raw_replacement_manifest_sqlite_match_unproven:
                replacement_manifest_sqlite_match_unproven,
            recent_raw_replacement_sqlite_inspection_error: sqlite_content.error,
            recent_raw_replacement_candidate_complete_against_current_source: replacement_promotion
                .recent_raw_replacement_candidate_complete_against_current_source,
            recent_raw_replacement_promotion_reason_class: replacement_promotion
                .recent_raw_replacement_promotion_reason_class,
        })
    }

}
