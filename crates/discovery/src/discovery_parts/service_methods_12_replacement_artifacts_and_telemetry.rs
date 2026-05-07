use super::*;

impl DiscoveryService {
    pub fn explain_recent_raw_replacement_artifact_history_contract_read_only(
        state_root: &Path,
    ) -> Result<RecentRawReplacementArtifactHistoryContractDiagnostic> {
        let state = Self::load_recent_raw_diagnostic_state_read_only(state_root);
        let staged_candidate_scan =
            Self::recent_raw_staged_candidate_reads_result(&state.snapshot_dir);
        let staged_candidate_scan_error = staged_candidate_scan
            .as_ref()
            .err()
            .map(|error| format!("{error:#}"));
        let staged_candidates = staged_candidate_scan.unwrap_or_default();
        let previous_artifact_candidates = staged_candidates
            .iter()
            .filter(|candidate| candidate.snapshot_path != state.staged_snapshot_path)
            .collect::<Vec<_>>();
        let previous_artifact_archive_candidate_count = previous_artifact_candidates.len();
        let previous_artifact_archive_parseable_count = previous_artifact_candidates
            .iter()
            .filter(|candidate| candidate.surface.manifest.is_some())
            .count();
        let previous_artifact_archive_snapshot_paths = previous_artifact_candidates
            .iter()
            .map(|candidate| candidate.snapshot_path.display().to_string())
            .collect::<Vec<_>>();
        let previous_artifact_archive_metadata_paths = previous_artifact_candidates
            .iter()
            .map(|candidate| candidate.metadata_path.display().to_string())
            .collect::<Vec<_>>();
        let current_fixed_candidate_exists =
            state.staged.snapshot_present && state.staged.metadata_present;
        let current_fixed_candidate_manifest_parseable = state.staged.manifest.is_some();
        let previous_artifact_archive_path_present = previous_artifact_archive_candidate_count > 0;

        let (reason_class, observed, missing_due_to_unproven_evidence, explanation) =
            Self::classify_recent_raw_replacement_artifact_history_contract(
                staged_candidate_scan_error.as_deref(),
                current_fixed_candidate_exists,
                current_fixed_candidate_manifest_parseable,
                previous_artifact_archive_candidate_count,
                previous_artifact_archive_parseable_count,
            );
        let previous_artifact_history_expected_under_current_contract = reason_class
            == RecentRawReplacementArtifactHistoryReasonClass::RecentRawReplacementArtifactHistoryFixedPathOverwriteByDesign;

        Ok(RecentRawReplacementArtifactHistoryContractDiagnostic {
            recent_raw_snapshot_dir: state.snapshot_dir.display().to_string(),
            recent_raw_replacement_artifact_history_observed: observed,
            recent_raw_replacement_artifact_history_reason_class: reason_class,
            recent_raw_replacement_artifact_history_explanation: explanation,
            recent_raw_replacement_fixed_path_overwrite_contract: true,
            recent_raw_replacement_fixed_snapshot_path: state
                .staged_snapshot_path
                .display()
                .to_string(),
            recent_raw_replacement_fixed_metadata_path: state
                .staged_metadata_path
                .display()
                .to_string(),
            recent_raw_replacement_current_fixed_candidate_exists: current_fixed_candidate_exists,
            recent_raw_replacement_current_fixed_candidate_manifest_parseable:
                current_fixed_candidate_manifest_parseable,
            recent_raw_replacement_staged_candidate_scan_succeeded: staged_candidate_scan_error
                .is_none(),
            recent_raw_replacement_staged_candidate_scan_error: staged_candidate_scan_error,
            recent_raw_replacement_previous_artifact_archive_path_present:
                previous_artifact_archive_path_present,
            recent_raw_replacement_previous_artifact_archive_candidate_count:
                previous_artifact_archive_candidate_count,
            recent_raw_replacement_previous_artifact_archive_parseable_count:
                previous_artifact_archive_parseable_count,
            recent_raw_replacement_previous_artifact_archive_snapshot_paths:
                previous_artifact_archive_snapshot_paths,
            recent_raw_replacement_previous_artifact_archive_metadata_paths:
                previous_artifact_archive_metadata_paths,
            recent_raw_replacement_previous_artifact_history_expected_under_current_contract:
                previous_artifact_history_expected_under_current_contract,
            recent_raw_replacement_previous_artifact_history_missing_due_to_unproven_evidence:
                missing_due_to_unproven_evidence,
        })
    }

    pub fn explain_recent_raw_replacement_attempt_telemetry_read_only(
        state_root: &Path,
    ) -> Result<RecentRawReplacementAttemptTelemetryDiagnostic> {
        Self::explain_recent_raw_replacement_attempt_telemetry_with_deep_scan_read_only(
            state_root, false,
        )
    }

    pub fn explain_recent_raw_replacement_attempt_telemetry_with_deep_scan_read_only(
        state_root: &Path,
        deep_scan: bool,
    ) -> Result<RecentRawReplacementAttemptTelemetryDiagnostic> {
        let snapshot_dir = Self::recent_raw_snapshot_dir_for_state_root(state_root);
        let (explicit_paths_checked, explicit_reads) =
            Self::recent_raw_replacement_attempt_telemetry_explicit_reads(
                state_root,
                &snapshot_dir,
            );
        let (scanned_dirs, deep_reads, scan_truncated) = if deep_scan {
            Self::recent_raw_replacement_attempt_telemetry_deep_scan_reads(
                state_root,
                &snapshot_dir,
            )
        } else {
            (Vec::new(), Vec::new(), false)
        };
        let mut telemetry_reads = Vec::new();
        let mut read_paths = BTreeSet::new();
        for read in explicit_reads.into_iter().chain(deep_reads) {
            if read_paths.insert(read.path.display().to_string()) {
                telemetry_reads.push(read);
            }
        }
        let probe_mode = if deep_scan {
            RECENT_RAW_ATTEMPT_TELEMETRY_PROBE_MODE_DEEP_SCAN
        } else {
            RECENT_RAW_ATTEMPT_TELEMETRY_PROBE_MODE_EXPLICIT_PATHS
        };
        let artifact_count = telemetry_reads.len();
        let parseable_count = telemetry_reads
            .iter()
            .filter(|read| read.telemetry.is_some())
            .count();
        let latest_read = telemetry_reads
            .iter()
            .filter(|read| read.telemetry.is_some())
            .max_by(|left, right| {
                Self::recent_raw_replacement_attempt_telemetry_timestamp(left)
                    .cmp(&Self::recent_raw_replacement_attempt_telemetry_timestamp(
                        right,
                    ))
                    .then_with(|| left.path.cmp(&right.path))
            });
        let latest_telemetry = latest_read.and_then(|read| read.telemetry.as_ref());
        let latest_timestamp =
            latest_read.and_then(Self::recent_raw_replacement_attempt_telemetry_timestamp);
        let latest_path = latest_read.map(|read| read.path.display().to_string());
        let latest_state = latest_telemetry.and_then(|telemetry| telemetry.state.clone());

        let row_count_advanced = latest_telemetry.and_then(|telemetry| {
            match (
                telemetry.staged_row_count_before_attempt,
                telemetry.staged_row_count_after_attempt,
            ) {
                (Some(before), Some(after)) => Some(after > before),
                _ => None,
            }
        });
        let row_count_stalled = latest_telemetry.and_then(|telemetry| {
            match (
                telemetry.staged_row_count_before_attempt,
                telemetry.staged_row_count_after_attempt,
            ) {
                (Some(before), Some(after)) => Some(after == before),
                _ => None,
            }
        });
        let covered_through_advanced = latest_telemetry.map(|telemetry| {
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
        let covered_through_stalled = latest_telemetry.map(|telemetry| {
            Self::recent_raw_optional_cursor_equal(
                telemetry
                    .staged_covered_through_cursor_before_attempt
                    .as_ref(),
                telemetry
                    .staged_covered_through_cursor_after_attempt
                    .as_ref(),
            )
        });
        let staged_progress_advanced =
            latest_telemetry.and_then(|telemetry| telemetry.staged_progress_advanced);
        let staged_progress_resumed =
            latest_telemetry.and_then(|telemetry| telemetry.staged_progress_resumed);
        let staged_progress_preserved_for_retry =
            latest_telemetry.and_then(|telemetry| telemetry.staged_progress_preserved_for_retry);

        let proves_reset_or_recreated = latest_telemetry
            .is_some_and(|telemetry| telemetry.staged_seeded_from_latest_surface == Some(true));
        let proves_advancing = latest_telemetry.is_some_and(|telemetry| {
            telemetry.state.as_deref() == Some("deferred")
                && telemetry.staged_progress_advanced == Some(true)
                && (row_count_advanced == Some(true) || covered_through_advanced == Some(true))
        });
        let proves_stalled = latest_telemetry.is_some_and(|telemetry| {
            telemetry.staged_progress_resumed == Some(true)
                && telemetry.staged_progress_advanced == Some(false)
                && row_count_stalled == Some(true)
                && covered_through_stalled == Some(true)
        });

        let (reason_class, observed, explanation) =
            Self::classify_recent_raw_replacement_attempt_telemetry(
                artifact_count,
                parseable_count,
                probe_mode,
                deep_scan,
                explicit_paths_checked.len(),
                scan_truncated,
                latest_state.as_deref(),
                latest_path.as_deref(),
                proves_advancing,
                proves_stalled,
                proves_reset_or_recreated,
            );

        Ok(RecentRawReplacementAttemptTelemetryDiagnostic {
            recent_raw_snapshot_dir: snapshot_dir.display().to_string(),
            recent_raw_replacement_attempt_telemetry_observed: observed,
            recent_raw_replacement_attempt_telemetry_reason_class: reason_class,
            recent_raw_replacement_attempt_telemetry_explanation: explanation,
            recent_raw_replacement_attempt_telemetry_probe_bounded: !deep_scan,
            recent_raw_replacement_attempt_telemetry_probe_mode: probe_mode.to_string(),
            recent_raw_replacement_attempt_telemetry_deep_scan_used: deep_scan,
            recent_raw_replacement_attempt_telemetry_explicit_paths_checked: explicit_paths_checked,
            recent_raw_replacement_attempt_telemetry_scanned_dirs: scanned_dirs,
            recent_raw_replacement_attempt_telemetry_scan_file_limit: if deep_scan {
                RECENT_RAW_ATTEMPT_TELEMETRY_SCAN_FILE_LIMIT
            } else {
                0
            },
            recent_raw_replacement_attempt_telemetry_scan_truncated: scan_truncated,
            recent_raw_replacement_attempt_telemetry_artifact_count: artifact_count,
            recent_raw_replacement_attempt_telemetry_parseable_count: parseable_count,
            recent_raw_replacement_attempt_telemetry_latest_path: latest_path,
            recent_raw_replacement_attempt_telemetry_latest_state: latest_state,
            recent_raw_replacement_attempt_telemetry_latest_timestamp: latest_timestamp,
            recent_raw_replacement_attempt_telemetry_proves_advancing: proves_advancing,
            recent_raw_replacement_attempt_telemetry_proves_stalled: proves_stalled,
            recent_raw_replacement_attempt_telemetry_proves_reset_or_recreated:
                proves_reset_or_recreated,
            recent_raw_replacement_attempt_telemetry_last_row_count_before: latest_telemetry
                .and_then(|telemetry| telemetry.staged_row_count_before_attempt),
            recent_raw_replacement_attempt_telemetry_last_row_count_after: latest_telemetry
                .and_then(|telemetry| telemetry.staged_row_count_after_attempt),
            recent_raw_replacement_attempt_telemetry_last_covered_through_before: latest_telemetry
                .and_then(|telemetry| {
                    telemetry
                        .staged_covered_through_cursor_before_attempt
                        .clone()
                }),
            recent_raw_replacement_attempt_telemetry_last_covered_through_after: latest_telemetry
                .and_then(|telemetry| {
                    telemetry
                        .staged_covered_through_cursor_after_attempt
                        .clone()
                }),
            recent_raw_replacement_attempt_telemetry_staged_progress_advanced:
                staged_progress_advanced,
            recent_raw_replacement_attempt_telemetry_staged_progress_resumed:
                staged_progress_resumed,
            recent_raw_replacement_attempt_telemetry_staged_progress_preserved_for_retry:
                staged_progress_preserved_for_retry,
        })
    }

}
