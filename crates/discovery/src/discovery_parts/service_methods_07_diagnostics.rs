use super::*;

impl DiscoveryService {
    pub fn explain_recent_raw_promotion_blocker_read_only(
        state_root: &Path,
    ) -> Result<RecentRawPromotionBlockerDiagnostic> {
        let state = Self::load_recent_raw_diagnostic_state_read_only(state_root);
        let staged_newer_than_promoted = match state.staged_vs_promoted_relation {
            Some(RecentRawManifestProgressRelation::CandidateAhead) => Some(true),
            Some(
                RecentRawManifestProgressRelation::ReferenceAhead
                | RecentRawManifestProgressRelation::Equivalent,
            ) => Some(false),
            None => None,
        };

        let (reason_class, blocker_observed, ready_now, explanation) =
            Self::classify_recent_raw_promotion_blocker(
                &state.promoted,
                &state.staged,
                state.promoted_exists,
                state.staged_exists,
                state.source_state_available,
                state.source_outruns_promoted,
                state.source_outruns_staged,
                staged_newer_than_promoted,
            );

        let promoted_current =
            state.promoted_exists && state.source_outruns_promoted == Some(false);
        Ok(RecentRawPromotionBlockerDiagnostic {
            recent_raw_snapshot_dir: state.snapshot_dir.display().to_string(),
            recent_raw_promotion_blocker_observed: blocker_observed,
            recent_raw_promotion_ready_now: ready_now,
            recent_raw_promotion_reason_class: reason_class,
            recent_raw_promotion_explanation: explanation,
            recent_raw_promoted_exists: state.promoted_exists,
            recent_raw_promoted_snapshot_present: state.promoted.snapshot_present,
            recent_raw_promoted_metadata_present: state.promoted.metadata_present,
            recent_raw_promoted_created_at: state
                .promoted
                .manifest
                .as_ref()
                .map(|manifest| manifest.created_at.clone()),
            recent_raw_promoted_covered_since: state
                .promoted
                .manifest
                .as_ref()
                .and_then(|manifest| manifest.covered_since.clone()),
            recent_raw_promoted_covered_through: state
                .promoted
                .manifest
                .as_ref()
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_promoted_row_count: state
                .promoted
                .manifest
                .as_ref()
                .map(|manifest| manifest.row_count),
            recent_raw_promoted_last_batch_completed_at: state
                .promoted
                .manifest
                .as_ref()
                .and_then(|manifest| manifest.last_batch_completed_at.clone()),
            recent_raw_staged_exists: state.staged_exists,
            recent_raw_staged_snapshot_present: state.staged.snapshot_present,
            recent_raw_staged_metadata_present: state.staged.metadata_present,
            recent_raw_staged_created_at: state
                .staged
                .manifest
                .as_ref()
                .map(|manifest| manifest.created_at.clone()),
            recent_raw_staged_covered_since: state
                .staged
                .manifest
                .as_ref()
                .and_then(|manifest| manifest.covered_since.clone()),
            recent_raw_staged_covered_through: state
                .staged
                .manifest
                .as_ref()
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_staged_row_count: state
                .staged
                .manifest
                .as_ref()
                .map(|manifest| manifest.row_count),
            recent_raw_staged_last_batch_completed_at: state
                .staged
                .manifest
                .as_ref()
                .and_then(|manifest| manifest.last_batch_completed_at.clone()),
            recent_raw_staged_newer_than_promoted: staged_newer_than_promoted,
            recent_raw_runtime_db_path: state.runtime_db_path,
            recent_raw_runtime_db_size_bytes: state.runtime_db_size_bytes,
            recent_raw_runtime_db_mtime: state.runtime_db_mtime,
            recent_raw_runtime_db_wal_present: state.runtime_db_wal_present,
            recent_raw_runtime_db_wal_size_bytes: state.runtime_db_wal_size_bytes,
            recent_raw_source_state_available: state.source_state_available,
            recent_raw_source_row_count: state.source_state.as_ref().map(|state| state.row_count),
            recent_raw_source_covered_since: state
                .source_state
                .as_ref()
                .and_then(|state| state.covered_since.clone()),
            recent_raw_source_covered_through: state
                .source_state
                .as_ref()
                .and_then(|state| state.covered_through_cursor.clone()),
            recent_raw_source_last_batch_completed_at: state
                .source_state
                .as_ref()
                .and_then(|state| state.last_batch_completed_at.clone()),
            recent_raw_source_outruns_staged: state.source_outruns_staged,
            recent_raw_source_outruns_promoted: state.source_outruns_promoted,
            recent_raw_stage3_truth_blocked_by_promotion: !promoted_current,
            recent_raw_stage3_current_fresh_healthy_evidence_possible: promoted_current,
        })
    }

    pub fn explain_recent_raw_catch_up_status_read_only(
        state_root: &Path,
    ) -> Result<RecentRawCatchUpDiagnostic> {
        let state = Self::load_recent_raw_diagnostic_state_read_only(state_root);
        let promoted_manifest = state.promoted.manifest.as_ref();
        let staged_manifest = state.staged.manifest.as_ref();
        let source_state = state.source_state.as_ref();
        let staged_ahead_of_promoted = match state.staged_vs_promoted_relation {
            Some(RecentRawManifestProgressRelation::CandidateAhead) => Some(true),
            Some(
                RecentRawManifestProgressRelation::ReferenceAhead
                | RecentRawManifestProgressRelation::Equivalent,
            ) => Some(false),
            None => None,
        };
        let staged_advancing = match state.staged_vs_promoted_relation {
            Some(RecentRawManifestProgressRelation::CandidateAhead) => Some(true),
            Some(
                RecentRawManifestProgressRelation::ReferenceAhead
                | RecentRawManifestProgressRelation::Equivalent,
            ) => Some(false),
            None => None,
        };
        let staged_last_batch_completed_at_newer_than_promoted =
            Self::recent_raw_optional_ts_newer_than(
                staged_manifest.and_then(|manifest| manifest.last_batch_completed_at.as_ref()),
                promoted_manifest.and_then(|manifest| manifest.last_batch_completed_at.as_ref()),
            );

        let source_vs_staged_row_lag = Self::recent_raw_nonnegative_row_lag(
            source_state.map(|state| state.row_count),
            staged_manifest.map(|manifest| manifest.row_count),
        );
        let source_vs_promoted_row_lag = Self::recent_raw_nonnegative_row_lag(
            source_state.map(|state| state.row_count),
            promoted_manifest.map(|manifest| manifest.row_count),
        );
        let source_vs_staged_time_lag_seconds =
            Self::recent_raw_nonnegative_cursor_time_lag_seconds(
                source_state.and_then(|state| state.covered_through_cursor.as_ref()),
                staged_manifest.and_then(|manifest| manifest.covered_through_cursor.as_ref()),
            );
        let source_vs_promoted_time_lag_seconds =
            Self::recent_raw_nonnegative_cursor_time_lag_seconds(
                source_state.and_then(|state| state.covered_through_cursor.as_ref()),
                promoted_manifest.and_then(|manifest| manifest.covered_through_cursor.as_ref()),
            );
        let staged_vs_promoted_row_delta = Self::recent_raw_signed_row_delta(
            staged_manifest.map(|manifest| manifest.row_count),
            promoted_manifest.map(|manifest| manifest.row_count),
        );
        let staged_vs_promoted_time_delta_seconds =
            Self::recent_raw_signed_cursor_time_delta_seconds(
                staged_manifest.and_then(|manifest| manifest.covered_through_cursor.as_ref()),
                promoted_manifest.and_then(|manifest| manifest.covered_through_cursor.as_ref()),
            );

        let (reason_class, observed, progressing, losing_to_source, indeterminate, explanation) =
            Self::classify_recent_raw_catch_up_status(&state);

        Ok(RecentRawCatchUpDiagnostic {
            recent_raw_snapshot_dir: state.snapshot_dir.display().to_string(),
            recent_raw_catch_up_status_observed: observed,
            recent_raw_catch_up_reason_class: reason_class,
            recent_raw_catch_up_explanation: explanation,
            recent_raw_promoted_exists: state.promoted_exists,
            recent_raw_promoted_created_at: promoted_manifest
                .map(|manifest| manifest.created_at.clone()),
            recent_raw_promoted_covered_through: promoted_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_promoted_row_count: promoted_manifest.map(|manifest| manifest.row_count),
            recent_raw_promoted_last_batch_completed_at: promoted_manifest
                .and_then(|manifest| manifest.last_batch_completed_at.clone()),
            recent_raw_staged_exists: state.staged_exists,
            recent_raw_staged_created_at: staged_manifest
                .map(|manifest| manifest.created_at.clone()),
            recent_raw_staged_covered_through: staged_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_staged_row_count: staged_manifest.map(|manifest| manifest.row_count),
            recent_raw_staged_last_batch_completed_at: staged_manifest
                .and_then(|manifest| manifest.last_batch_completed_at.clone()),
            recent_raw_staged_advancing: staged_advancing,
            recent_raw_staged_ahead_of_promoted: staged_ahead_of_promoted,
            recent_raw_staged_last_batch_completed_at_newer_than_promoted:
                staged_last_batch_completed_at_newer_than_promoted,
            recent_raw_source_state_available: state.source_state_available,
            recent_raw_source_covered_through: source_state
                .and_then(|state| state.covered_through_cursor.clone()),
            recent_raw_source_row_count: source_state.map(|state| state.row_count),
            recent_raw_source_last_batch_completed_at: source_state
                .and_then(|state| state.last_batch_completed_at.clone()),
            recent_raw_runtime_db_path: state.runtime_db_path,
            recent_raw_runtime_db_size_bytes: state.runtime_db_size_bytes,
            recent_raw_runtime_db_mtime: state.runtime_db_mtime,
            recent_raw_runtime_db_wal_present: state.runtime_db_wal_present,
            recent_raw_runtime_db_wal_size_bytes: state.runtime_db_wal_size_bytes,
            recent_raw_source_vs_staged_row_lag: source_vs_staged_row_lag,
            recent_raw_source_vs_promoted_row_lag: source_vs_promoted_row_lag,
            recent_raw_source_vs_staged_time_lag_seconds: source_vs_staged_time_lag_seconds,
            recent_raw_source_vs_promoted_time_lag_seconds: source_vs_promoted_time_lag_seconds,
            recent_raw_staged_vs_promoted_row_delta: staged_vs_promoted_row_delta,
            recent_raw_staged_vs_promoted_time_delta_seconds: staged_vs_promoted_time_delta_seconds,
            recent_raw_catch_up_progressing: progressing,
            recent_raw_catch_up_losing_to_source: losing_to_source,
            recent_raw_catch_up_indeterminate: indeterminate,
        })
    }
}
