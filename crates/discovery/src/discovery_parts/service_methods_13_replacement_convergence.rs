impl DiscoveryService {
    pub fn explain_recent_raw_replacement_convergence_read_only(
        state_root: &Path,
    ) -> Result<RecentRawReplacementConvergenceDiagnostic> {
        let snapshot_dir = Self::recent_raw_snapshot_dir_for_state_root(state_root);
        let promoted_snapshot_path = runtime_artifacts::journal_snapshot_latest_path(&snapshot_dir);
        let promoted_metadata_path =
            runtime_artifacts::journal_snapshot_latest_metadata_path(&snapshot_dir);
        let staged_snapshot_path = Self::recent_raw_staged_snapshot_path(&snapshot_dir);
        let staged_metadata_path = Self::recent_raw_staged_metadata_path(&snapshot_dir);
        let promoted = Self::read_recent_raw_surface_manifest(
            &promoted_snapshot_path,
            &promoted_metadata_path,
        );
        let staged =
            Self::read_recent_raw_surface_manifest(&staged_snapshot_path, &staged_metadata_path);
        let promoted_manifest = promoted.manifest.as_ref();
        let staged_manifest = staged.manifest.as_ref();
        let promoted_exists = promoted_manifest.is_some();
        let replacement_candidate_exists = staged_manifest.is_some();

        let source_runtime_db_path =
            promoted_manifest.map(|manifest| PathBuf::from(&manifest.source_db_path));
        let (source_state, source_state_available) = match source_runtime_db_path.as_ref() {
            Some(source_runtime_db_path) => {
                match Self::load_recent_raw_source_state_read_only(source_runtime_db_path) {
                    Ok(source_state) => (Some(source_state), true),
                    Err(_) => (None, false),
                }
            }
            None => (None, false),
        };
        let source_state = source_state.as_ref();
        let source_row_count = source_state.map(|state| state.row_count);

        let replacement_candidate_source_db_matches_promoted =
            match (staged_manifest, promoted_manifest) {
                (Some(staged_manifest), Some(promoted_manifest)) => {
                    Some(staged_manifest.source_db_path == promoted_manifest.source_db_path)
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
            replacement_candidate_source_db_matches_promoted,
            source_state,
            staged_manifest,
        ) {
            (Some(true), Some(source_state), Some(staged_manifest)) => Some(
                Self::recent_raw_source_outruns_manifest(source_state, staged_manifest),
            ),
            _ => None,
        };
        let staged_vs_promoted_relation = match (staged_manifest, promoted_manifest) {
            (Some(staged_manifest), Some(promoted_manifest)) => {
                Self::recent_raw_manifest_progress_relation(staged_manifest, promoted_manifest)
            }
            _ => None,
        };
        let staged_newer_than_promoted = match staged_vs_promoted_relation {
            Some(RecentRawManifestProgressRelation::CandidateAhead) => Some(true),
            Some(
                RecentRawManifestProgressRelation::ReferenceAhead
                | RecentRawManifestProgressRelation::Equivalent,
            ) => Some(false),
            None => None,
        };
        let candidate_complete_against_current_source =
            match (replacement_candidate_exists, source_outruns_staged) {
                (true, Some(source_outruns_staged)) => Some(!source_outruns_staged),
                _ => None,
            };
        let (
            promotion_reason_class,
            _promotion_blocker_observed,
            _promotion_ready_now,
            _promotion_explanation,
        ) = Self::classify_recent_raw_promotion_blocker(
            &promoted,
            &staged,
            promoted_exists,
            replacement_candidate_exists,
            source_state_available,
            source_outruns_promoted,
            source_outruns_staged,
            staged_newer_than_promoted,
        );
        let candidate_promotable_now = if !replacement_candidate_exists {
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
        let candidate_row_count = staged_manifest.map(|manifest| manifest.row_count);
        let rows_remaining_to_current_source =
            Self::recent_raw_nonnegative_row_lag(source_row_count, candidate_row_count);

        let telemetry_path =
            Self::recent_raw_replacement_attempt_telemetry_latest_path(&snapshot_dir);
        let telemetry_read =
            Self::recent_raw_replacement_attempt_telemetry_read_path(&telemetry_path);
        let telemetry = telemetry_read
            .as_ref()
            .and_then(|read| read.telemetry.as_ref());
        let attempt_telemetry_parseable = telemetry.is_some();
        let latest_attempt_row_count_before =
            telemetry.and_then(|telemetry| telemetry.staged_row_count_before_attempt);
        let latest_attempt_row_count_after =
            telemetry.and_then(|telemetry| telemetry.staged_row_count_after_attempt);
        let latest_attempt_row_count_delta = match (
            latest_attempt_row_count_before,
            latest_attempt_row_count_after,
        ) {
            (Some(before), Some(after)) => {
                let after = i128::try_from(after).ok();
                let before = i128::try_from(before).ok();
                match (after, before) {
                    (Some(after), Some(before)) => {
                        Some((after - before).clamp(i64::MIN as i128, i64::MAX as i128) as i64)
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        let latest_attempt_covered_through_before = telemetry.and_then(|telemetry| {
            telemetry
                .staged_covered_through_cursor_before_attempt
                .clone()
        });
        let latest_attempt_covered_through_after = telemetry.and_then(|telemetry| {
            telemetry
                .staged_covered_through_cursor_after_attempt
                .clone()
        });
        let latest_attempt_row_count_advanced =
            latest_attempt_row_count_delta.map(|delta| delta > 0);
        let latest_attempt_covered_through_advanced = telemetry.map(|telemetry| {
            Self::recent_raw_lineage_relation_from_cursor(
                telemetry
                    .staged_covered_through_cursor_after_attempt
                    .as_ref(),
                telemetry
                    .staged_covered_through_cursor_before_attempt
                    .as_ref(),
                Some(true),
            ) == RecentRawLineageRelation::Ahead
        });
        let latest_attempt_advanced = match (
            latest_attempt_row_count_advanced,
            latest_attempt_covered_through_advanced,
        ) {
            (Some(row_count_advanced), Some(covered_through_advanced)) => {
                Some(row_count_advanced || covered_through_advanced)
            }
            (Some(row_count_advanced), None) => Some(row_count_advanced),
            (None, Some(covered_through_advanced)) => Some(covered_through_advanced),
            (None, None) => None,
        };
        let latest_attempt_covered_through_stalled = telemetry.map(|telemetry| {
            Self::recent_raw_optional_cursor_equal(
                telemetry
                    .staged_covered_through_cursor_before_attempt
                    .as_ref(),
                telemetry
                    .staged_covered_through_cursor_after_attempt
                    .as_ref(),
            )
        });
        let latest_attempt_stalled = match (
            telemetry.and_then(|telemetry| telemetry.staged_progress_resumed),
            telemetry.and_then(|telemetry| telemetry.staged_progress_advanced),
            latest_attempt_row_count_delta,
            latest_attempt_covered_through_stalled,
        ) {
            (Some(true), Some(false), Some(0), Some(true)) => Some(true),
            (Some(_), Some(_), Some(_), Some(_)) => Some(false),
            _ => None,
        };
        let latest_attempt_reset_or_recreated =
            telemetry.map(|telemetry| telemetry.staged_seeded_from_latest_surface == Some(true));
        let estimated_attempts_to_current_source = match (
            rows_remaining_to_current_source,
            latest_attempt_row_count_delta,
        ) {
            (Some(remaining), Some(delta)) if delta > 0 => {
                let delta = delta as u64;
                Some((remaining.saturating_add(delta).saturating_sub(1)) / delta)
            }
            (Some(0), _) => Some(0),
            _ => None,
        };

        let latest_attempt_resumed =
            telemetry.and_then(|telemetry| telemetry.staged_progress_resumed);
        let latest_attempt_preserved_for_retry =
            telemetry.and_then(|telemetry| telemetry.staged_progress_preserved_for_retry);

        let (reason_class, observed, explanation) =
            Self::classify_recent_raw_replacement_convergence(
                replacement_candidate_exists,
                source_state_available,
                candidate_complete_against_current_source,
                candidate_promotable_now,
                attempt_telemetry_parseable,
                latest_attempt_advanced,
                latest_attempt_stalled,
                latest_attempt_reset_or_recreated,
                rows_remaining_to_current_source,
                latest_attempt_row_count_delta,
                estimated_attempts_to_current_source,
            );

        Ok(RecentRawReplacementConvergenceDiagnostic {
            recent_raw_snapshot_dir: snapshot_dir.display().to_string(),
            recent_raw_replacement_convergence_observed: observed,
            recent_raw_replacement_convergence_reason_class: reason_class,
            recent_raw_replacement_convergence_explanation: explanation,
            recent_raw_replacement_candidate_exists: replacement_candidate_exists,
            recent_raw_replacement_candidate_row_count: candidate_row_count,
            recent_raw_source_row_count: source_row_count,
            recent_raw_replacement_rows_remaining_to_current_source:
                rows_remaining_to_current_source,
            recent_raw_replacement_latest_attempt_row_count_before: latest_attempt_row_count_before,
            recent_raw_replacement_latest_attempt_row_count_after: latest_attempt_row_count_after,
            recent_raw_replacement_latest_attempt_row_count_delta: latest_attempt_row_count_delta,
            recent_raw_replacement_latest_attempt_covered_through_before:
                latest_attempt_covered_through_before,
            recent_raw_replacement_latest_attempt_covered_through_after:
                latest_attempt_covered_through_after,
            recent_raw_replacement_latest_attempt_advanced: latest_attempt_advanced,
            recent_raw_replacement_latest_attempt_resumed: latest_attempt_resumed,
            recent_raw_replacement_latest_attempt_preserved_for_retry:
                latest_attempt_preserved_for_retry,
            recent_raw_replacement_estimated_attempts_to_current_source:
                estimated_attempts_to_current_source,
            recent_raw_replacement_candidate_complete_against_current_source:
                candidate_complete_against_current_source,
            recent_raw_replacement_candidate_promotable_now: candidate_promotable_now,
            recent_raw_replacement_attempt_telemetry_path: telemetry_path.display().to_string(),
            recent_raw_replacement_attempt_telemetry_parseable: attempt_telemetry_parseable,
            recent_raw_replacement_attempt_telemetry_probe_bounded: true,
        })
    }
}
