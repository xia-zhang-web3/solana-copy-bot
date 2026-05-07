use super::*;

impl DiscoveryService {
    pub fn explain_recent_raw_staged_window_seeding_read_only(
        state_root: &Path,
    ) -> Result<RecentRawStagedWindowSeedingDiagnostic> {
        let state = Self::load_recent_raw_diagnostic_state_read_only(state_root);
        let promoted_manifest = state.promoted.manifest.as_ref();
        let staged_manifest = state.staged.manifest.as_ref();
        let source_state = state.source_state.as_ref();

        let same_source_db_as_promoted = match (staged_manifest, promoted_manifest) {
            (Some(staged_manifest), Some(promoted_manifest)) => {
                Some(staged_manifest.source_db_path == promoted_manifest.source_db_path)
            }
            _ => None,
        };
        let staged_created_after_promoted = match (staged_manifest, promoted_manifest) {
            (Some(staged_manifest), Some(promoted_manifest)) => {
                Some(staged_manifest.created_at > promoted_manifest.created_at)
            }
            _ => None,
        };
        let staged_created_at_matches_promoted_created_at =
            match (staged_manifest, promoted_manifest) {
                (Some(staged_manifest), Some(promoted_manifest)) => {
                    Some(staged_manifest.created_at == promoted_manifest.created_at)
                }
                _ => None,
            };
        let staged_start_matches_promoted_start = match same_source_db_as_promoted {
            Some(true) => match (
                staged_manifest.and_then(|manifest| manifest.covered_since),
                promoted_manifest.and_then(|manifest| manifest.covered_since),
            ) {
                (Some(staged_since), Some(promoted_since)) => Some(staged_since == promoted_since),
                _ => None,
            },
            Some(false) | None => None,
        };
        let staged_start_matches_source_start = match (
            staged_manifest.and_then(|manifest| manifest.covered_since),
            source_state.and_then(|state| state.covered_since),
        ) {
            (Some(staged_since), Some(source_since)) => Some(staged_since == source_since),
            _ => None,
        };
        let staged_start_matches_current_window_cutoff = staged_start_matches_source_start;

        let promoted_seed_blocked_by_source_contract_mismatch =
            match (source_state, promoted_manifest) {
                (Some(source_state), Some(promoted_manifest)) => {
                    Some(Self::recent_raw_source_contract_no_longer_matches_manifest(
                        source_state,
                        promoted_manifest,
                    ))
                }
                _ => None,
            };
        let promoted_supersedes_staged_progress = match (promoted_manifest, staged_manifest) {
            (Some(promoted_manifest), Some(staged_manifest)) => Some(
                Self::recent_raw_published_latest_supersedes_staged_progress(
                    promoted_manifest,
                    staged_manifest,
                ),
            ),
            _ => None,
        };
        let promoted_can_seed_staged_progress_under_current_code = match (
            state.runtime_db_path.as_ref(),
            source_state,
            promoted_manifest,
        ) {
            (Some(runtime_db_path), Some(source_state), Some(promoted_manifest)) => {
                Some(Self::recent_raw_latest_surface_can_seed_staged_progress(
                    Path::new(runtime_db_path),
                    source_state,
                    promoted_manifest,
                    staged_manifest,
                ))
            }
            _ => None,
        };

        let sqlite_content =
            Self::read_recent_raw_snapshot_sqlite_content_read_only(&state.staged_snapshot_path);
        let sqlite_state = sqlite_content.state.as_ref();
        let staged_manifest_matches_sqlite_content = match (staged_manifest, sqlite_state) {
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
        let staged_manifest_sqlite_match_unproven =
            staged_manifest_matches_sqlite_content.is_none();

        let staged_start_matches_neither_promoted_nor_source = match (
            same_source_db_as_promoted,
            staged_start_matches_promoted_start,
            staged_start_matches_current_window_cutoff,
        ) {
            (Some(true), Some(false), Some(false)) => Some(true),
            (Some(true), Some(true), _) | (Some(true), _, Some(true)) => Some(false),
            _ => None,
        };

        let (
            reason_class,
            observed,
            current_evidence_basis,
            explanation,
            current_evidence_explanation,
        ) = Self::classify_recent_raw_staged_window_seeding(
            &state,
            same_source_db_as_promoted,
            staged_start_matches_promoted_start,
            staged_start_matches_current_window_cutoff,
            staged_start_matches_neither_promoted_nor_source,
            staged_manifest_matches_sqlite_content,
            sqlite_content.error.as_deref(),
        );

        Ok(RecentRawStagedWindowSeedingDiagnostic {
            recent_raw_snapshot_dir: state.snapshot_dir.display().to_string(),
            recent_raw_staged_window_seeding_observed: observed,
            recent_raw_staged_window_seeding_reason_class: reason_class,
            recent_raw_staged_window_seeding_explanation: explanation,
            recent_raw_staged_window_historical_seeding_basis_proven_from_current_artifacts: false,
            recent_raw_promoted_covered_since: promoted_manifest
                .and_then(|manifest| manifest.covered_since),
            recent_raw_staged_covered_since: staged_manifest
                .and_then(|manifest| manifest.covered_since),
            recent_raw_source_covered_since: source_state.and_then(|state| state.covered_since),
            recent_raw_promoted_covered_through: promoted_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_staged_covered_through: staged_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_source_covered_through: source_state
                .and_then(|state| state.covered_through_cursor.clone()),
            recent_raw_staged_start_matches_promoted_start: staged_start_matches_promoted_start,
            recent_raw_staged_start_matches_source_start: staged_start_matches_source_start,
            recent_raw_staged_start_matches_current_window_cutoff:
                staged_start_matches_current_window_cutoff,
            recent_raw_staged_start_matches_neither_promoted_nor_source:
                staged_start_matches_neither_promoted_nor_source,
            recent_raw_staged_start_current_evidence_basis: current_evidence_basis,
            recent_raw_staged_start_current_evidence_explanation: current_evidence_explanation,
            recent_raw_staged_same_source_db_as_promoted: same_source_db_as_promoted,
            recent_raw_staged_created_after_promoted: staged_created_after_promoted,
            recent_raw_staged_created_at_matches_promoted_created_at:
                staged_created_at_matches_promoted_created_at,
            recent_raw_promoted_can_seed_staged_progress_under_current_code:
                promoted_can_seed_staged_progress_under_current_code,
            recent_raw_promoted_seed_blocked_by_source_contract_mismatch:
                promoted_seed_blocked_by_source_contract_mismatch,
            recent_raw_promoted_supersedes_staged_progress: promoted_supersedes_staged_progress,
            recent_raw_staged_manifest_matches_sqlite_content:
                staged_manifest_matches_sqlite_content,
            recent_raw_staged_manifest_sqlite_match_unproven: staged_manifest_sqlite_match_unproven,
        })
    }
}
