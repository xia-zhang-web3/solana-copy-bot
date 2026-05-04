impl DiscoveryService {
    pub fn explain_recent_raw_promoted_retention_contract_read_only(
        state_root: &Path,
    ) -> Result<RecentRawPromotedRetentionContractDiagnostic> {
        let state = Self::load_recent_raw_diagnostic_state_read_only(state_root);
        let promoted_manifest = state.promoted.manifest.as_ref();
        let staged_manifest = state.staged.manifest.as_ref();
        let source_window_contract =
            Self::explain_recent_raw_source_window_contract_read_only(state_root)?;

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
        let promoted_same_source_db_as_current_source =
            source_window_contract.recent_raw_source_same_source_db_as_promoted;
        let promoted_start_older_than_current_source = match (
            promoted_same_source_db_as_current_source,
            source_window_contract.recent_raw_source_window_matches_current_bounded_contract,
            promoted_manifest.and_then(|manifest| manifest.covered_since),
            source_state.and_then(|state| state.covered_since),
        ) {
            (Some(true), Some(true), Some(promoted_since), Some(source_since)) => {
                Some(promoted_since < source_since)
            }
            _ => None,
        };
        let source_outruns_promoted = match (source_state, promoted_manifest) {
            (Some(source_state), Some(promoted_manifest)) => Some(
                Self::recent_raw_source_outruns_manifest(source_state, promoted_manifest),
            ),
            _ => None,
        };
        let source_outruns_staged = match (
            promoted_same_source_db_as_current_source,
            source_state,
            staged_manifest,
            promoted_manifest,
        ) {
            (Some(true), Some(source_state), Some(staged_manifest), Some(promoted_manifest))
                if staged_manifest.source_db_path == promoted_manifest.source_db_path =>
            {
                Some(Self::recent_raw_source_outruns_manifest(
                    source_state,
                    staged_manifest,
                ))
            }
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
        let promoted_currently_retained_as_truth = if state.promoted.manifest_error.is_some() {
            None
        } else {
            Self::recent_raw_promoted_retained_as_truth_under_current_contract(
                state.promoted_exists,
                promotion_reason_class,
            )
        };
        let promoted_has_current_contract_invalidation_rule = match (
            promoted_currently_retained_as_truth,
            promoted_same_source_db_as_current_source,
            source_window_contract.recent_raw_source_window_matches_current_bounded_contract,
        ) {
            (Some(true), Some(true), Some(true)) => Some(false),
            _ => None,
        };
        let promoted_invalidated_by_current_source_window_shift = match (
            promoted_currently_retained_as_truth,
            promoted_same_source_db_as_current_source,
            source_window_contract.recent_raw_source_window_matches_current_bounded_contract,
            promoted_has_current_contract_invalidation_rule,
        ) {
            (Some(true), Some(true), Some(true), Some(false)) => Some(false),
            _ => None,
        };
        let promoted_current = state.promoted_exists && source_outruns_promoted == Some(false);
        let stage3_truth_currently_depends_on_retained_older_promoted_surface = match (
            promoted_currently_retained_as_truth,
            promoted_start_older_than_current_source,
        ) {
            (Some(true), Some(older_than_source)) => Some(older_than_source && !promoted_current),
            _ => None,
        };
        let stage3_truth_can_advance_without_new_promotion =
            promoted_currently_retained_as_truth.map(|retained| retained && promoted_current);

        let (
            reason_class,
            observed,
            retention_basis,
            retention_explanation,
            retention_basis_explanation,
        ) = Self::classify_recent_raw_promoted_retention_contract(
            state.promoted.manifest_error.as_deref(),
            source_window_contract.recent_raw_source_window_contract_observed,
            source_window_contract.recent_raw_source_window_contract_reason_class,
            promotion_reason_class,
            promoted_same_source_db_as_current_source,
            promoted_currently_retained_as_truth,
            promoted_start_older_than_current_source,
            promoted_has_current_contract_invalidation_rule,
            promoted_invalidated_by_current_source_window_shift,
            stage3_truth_currently_depends_on_retained_older_promoted_surface,
            stage3_truth_can_advance_without_new_promotion,
        );

        Ok(RecentRawPromotedRetentionContractDiagnostic {
            recent_raw_snapshot_dir: state.snapshot_dir.display().to_string(),
            recent_raw_promoted_snapshot_path: state.promoted_snapshot_path.display().to_string(),
            recent_raw_promoted_metadata_path: state.promoted_metadata_path.display().to_string(),
            recent_raw_promoted_retention_observed: observed,
            recent_raw_promoted_retention_reason_class: reason_class,
            recent_raw_promoted_retention_explanation: retention_explanation,
            recent_raw_promoted_covered_since: promoted_manifest
                .and_then(|manifest| manifest.covered_since),
            recent_raw_source_covered_since: source_state.and_then(|state| state.covered_since),
            recent_raw_promoted_covered_through: promoted_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_source_covered_through: source_state
                .and_then(|state| state.covered_through_cursor.clone()),
            recent_raw_promoted_start_older_than_current_source:
                promoted_start_older_than_current_source,
            recent_raw_promoted_same_source_db_as_current_source:
                promoted_same_source_db_as_current_source,
            recent_raw_promoted_currently_retained_as_truth: promoted_currently_retained_as_truth,
            recent_raw_promoted_has_current_contract_invalidation_rule:
                promoted_has_current_contract_invalidation_rule,
            recent_raw_promoted_invalidated_by_current_source_window_shift:
                promoted_invalidated_by_current_source_window_shift,
            recent_raw_promoted_retention_basis: retention_basis,
            recent_raw_promoted_retention_basis_explanation: retention_basis_explanation,
            recent_raw_stage3_truth_currently_depends_on_retained_older_promoted_surface:
                stage3_truth_currently_depends_on_retained_older_promoted_surface,
            recent_raw_stage3_truth_can_advance_without_new_promotion:
                stage3_truth_can_advance_without_new_promotion,
            recent_raw_promoted_exists: state.promoted_exists,
            recent_raw_promoted_snapshot_present: state.promoted.snapshot_present,
            recent_raw_promoted_metadata_present: state.promoted.metadata_present,
            recent_raw_promotion_ready_now: promotion_ready_now,
            recent_raw_promotion_reason_class: promotion_reason_class,
            recent_raw_stage3_truth_blocked_by_promotion: !promoted_current,
            recent_raw_source_window_contract_observed: source_window_contract
                .recent_raw_source_window_contract_observed,
            recent_raw_source_window_contract_reason_class: source_window_contract
                .recent_raw_source_window_contract_reason_class,
        })
    }
}
