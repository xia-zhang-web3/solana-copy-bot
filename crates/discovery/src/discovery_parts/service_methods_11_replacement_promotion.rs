impl DiscoveryService {
    pub fn explain_recent_raw_replacement_promotion_contract_read_only(
        state_root: &Path,
    ) -> Result<RecentRawReplacementPromotionContractDiagnostic> {
        let state = Self::load_recent_raw_diagnostic_state_read_only(state_root);
        let promoted_manifest = state.promoted.manifest.as_ref();
        let staged_manifest = state.staged.manifest.as_ref();
        let promoted_retention =
            Self::explain_recent_raw_promoted_retention_contract_read_only(state_root)?;

        let source_runtime_db_path =
            promoted_manifest.map(|manifest| PathBuf::from(&manifest.source_db_path));
        let (source_state, current_source_state_available) = match source_runtime_db_path.as_ref() {
            Some(source_runtime_db_path) => {
                match Self::load_recent_raw_source_state_read_only(source_runtime_db_path) {
                    Ok(source_state) => (Some(source_state), true),
                    Err(_) => (None, false),
                }
            }
            None => (None, false),
        };
        let source_state = source_state.as_ref();

        let replacement_candidate_source_db_matches_promoted =
            match (staged_manifest, promoted_manifest) {
                (Some(staged_manifest), Some(promoted_manifest)) => {
                    Some(staged_manifest.source_db_path == promoted_manifest.source_db_path)
                }
                _ => None,
            };
        let replacement_candidate_start_matches_current_source = match (
            replacement_candidate_source_db_matches_promoted,
            staged_manifest.and_then(|manifest| manifest.covered_since),
            source_state.and_then(|state| state.covered_since),
        ) {
            (Some(true), Some(staged_since), Some(source_since)) => {
                Some(staged_since == source_since)
            }
            _ => None,
        };
        let replacement_candidate_covered_through_relation_to_promoted =
            Self::recent_raw_lineage_relation_from_cursor(
                staged_manifest.and_then(|manifest| manifest.covered_through_cursor.as_ref()),
                promoted_manifest.and_then(|manifest| manifest.covered_through_cursor.as_ref()),
                replacement_candidate_source_db_matches_promoted,
            );
        let replacement_candidate_row_count_relation_to_promoted =
            Self::recent_raw_lineage_relation_from_optional_row_count(
                staged_manifest.map(|manifest| manifest.row_count),
                promoted_manifest.map(|manifest| manifest.row_count),
                replacement_candidate_source_db_matches_promoted,
            );

        let source_outruns_promoted = match (source_state, promoted_manifest) {
            (Some(source_state), Some(promoted_manifest)) => Some(
                Self::recent_raw_source_outruns_manifest(source_state, promoted_manifest),
            ),
            _ => None,
        };
        let source_outruns_staged = match (
            replacement_candidate_source_db_matches_promoted,
            source_state,
            staged_manifest,
        ) {
            (Some(true), Some(source_state), Some(staged_manifest)) => Some(
                Self::recent_raw_source_outruns_manifest(source_state, staged_manifest),
            ),
            _ => None,
        };
        let replacement_candidate_complete_against_current_source =
            match (state.staged_exists, source_outruns_staged) {
                (true, Some(source_outruns_staged)) => Some(!source_outruns_staged),
                _ => None,
            };
        let staged_newer_than_promoted = match state.staged_vs_promoted_relation {
            Some(RecentRawManifestProgressRelation::CandidateAhead) => Some(true),
            Some(
                RecentRawManifestProgressRelation::ReferenceAhead
                | RecentRawManifestProgressRelation::Equivalent,
            ) => Some(false),
            None => None,
        };
        let (
            promotion_reason_class,
            _promotion_blocker_observed,
            promotion_ready_now,
            _promotion_explanation,
        ) = Self::classify_recent_raw_promotion_blocker(
            &state.promoted,
            &state.staged,
            state.promoted_exists,
            state.staged_exists,
            current_source_state_available,
            source_outruns_promoted,
            source_outruns_staged,
            staged_newer_than_promoted,
        );
        let replacement_candidate_promotable_now = if !state.staged_exists {
            None
        } else {
            match promotion_reason_class {
                RecentRawPromotionBlockerReasonClass::RecentRawPromotionReadyNow => Some(true),
                RecentRawPromotionBlockerReasonClass::RecentRawPromotionBlockedByUnprovenState => {
                    None
                }
                _ => Some(false),
            }
        };
        let replacement_promotion_would_retire_current_promoted_truth = match (
            state.promoted_exists,
            state.staged_exists,
            replacement_candidate_source_db_matches_promoted,
        ) {
            (false, _, _) => Some(false),
            (true, true, Some(true)) => Some(true),
            _ => None,
        };
        let stage3_blocked_on_replacement_candidate = match (
            promoted_retention
                .recent_raw_stage3_truth_currently_depends_on_retained_older_promoted_surface,
            promoted_retention.recent_raw_stage3_truth_can_advance_without_new_promotion,
            state.staged_exists,
            replacement_candidate_promotable_now,
        ) {
            (_, Some(true), _, _) => Some(false),
            (Some(true), Some(false), false, _) => Some(true),
            (Some(true), Some(false), true, Some(false)) => Some(true),
            (Some(true), Some(false), true, Some(true)) => Some(false),
            (Some(false), _, _, _) => Some(false),
            _ => None,
        };

        let (reason_class, observed, explanation) =
            Self::classify_recent_raw_replacement_promotion_contract(
                state.promoted.manifest_error.as_deref(),
                state.staged.manifest_error.as_deref(),
                state.promoted_exists,
                current_source_state_available,
                state.staged_exists,
                replacement_candidate_source_db_matches_promoted,
                replacement_candidate_start_matches_current_source,
                replacement_candidate_covered_through_relation_to_promoted,
                replacement_candidate_row_count_relation_to_promoted,
                replacement_candidate_complete_against_current_source,
                replacement_candidate_promotable_now,
                stage3_blocked_on_replacement_candidate,
                replacement_promotion_would_retire_current_promoted_truth,
                promotion_reason_class,
            );

        Ok(RecentRawReplacementPromotionContractDiagnostic {
            recent_raw_snapshot_dir: state.snapshot_dir.display().to_string(),
            recent_raw_replacement_promotion_observed: observed,
            recent_raw_replacement_promotion_reason_class: reason_class,
            recent_raw_replacement_promotion_explanation: explanation,
            recent_raw_replacement_candidate_exists: state.staged_exists,
            recent_raw_replacement_candidate_source_db_matches_promoted:
                replacement_candidate_source_db_matches_promoted,
            recent_raw_replacement_candidate_start_matches_current_source:
                replacement_candidate_start_matches_current_source,
            recent_raw_replacement_candidate_covered_through_relation_to_promoted:
                replacement_candidate_covered_through_relation_to_promoted,
            recent_raw_replacement_candidate_row_count_relation_to_promoted:
                replacement_candidate_row_count_relation_to_promoted,
            recent_raw_replacement_candidate_complete_against_current_source:
                replacement_candidate_complete_against_current_source,
            recent_raw_replacement_candidate_promotable_now: replacement_candidate_promotable_now,
            recent_raw_replacement_promotion_would_retire_current_promoted_truth:
                replacement_promotion_would_retire_current_promoted_truth,
            recent_raw_stage3_blocked_on_replacement_candidate:
                stage3_blocked_on_replacement_candidate,
            recent_raw_promotion_reason_class: promotion_reason_class,
            recent_raw_promotion_ready_now: promotion_ready_now,
        })
    }
}
