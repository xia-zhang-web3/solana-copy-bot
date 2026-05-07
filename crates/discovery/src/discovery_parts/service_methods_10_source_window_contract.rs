use super::*;

impl DiscoveryService {
    pub fn explain_recent_raw_source_window_contract_read_only(
        state_root: &Path,
    ) -> Result<RecentRawSourceWindowContractDiagnostic> {
        let state = Self::load_recent_raw_diagnostic_state_read_only(state_root);
        let promoted_manifest = state.promoted.manifest.as_ref();
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
        let same_source_db_as_promoted = match (source_runtime_db_path.as_ref(), promoted_manifest)
        {
            (Some(source_runtime_db_path), Some(promoted_manifest)) => Some(
                source_runtime_db_path.display().to_string() == promoted_manifest.source_db_path,
            ),
            _ => None,
        };

        let source_bounded_probe = source_runtime_db_path
            .as_ref()
            .map(|source_runtime_db_path| {
                Self::load_recent_raw_source_observed_swaps_bounded_probe_read_only(
                    source_runtime_db_path,
                )
            })
            .unwrap_or_default();
        let source_scan_error = source_bounded_probe.error.clone();
        let source_cached_state_matches_bounded_probe =
            match (source_state, source_scan_error.as_ref()) {
                (Some(source_state), None) => Some(
                    Self::recent_raw_source_state_matches_observed_swaps_bounded_probe(
                        source_state,
                        &source_bounded_probe,
                    ),
                ),
                _ => None,
            };
        let source_window_matches_current_bounded_contract =
            source_cached_state_matches_bounded_probe;
        let source_start_matches_promoted_start = match same_source_db_as_promoted {
            Some(true) => match (
                source_state.and_then(|state| state.covered_since),
                promoted_manifest.and_then(|manifest| manifest.covered_since),
            ) {
                (Some(source_since), Some(promoted_since)) => Some(source_since == promoted_since),
                _ => None,
            },
            Some(false) | None => None,
        };
        let source_start_later_than_promoted = match same_source_db_as_promoted {
            Some(true) => match (
                source_state.and_then(|state| state.covered_since),
                promoted_manifest.and_then(|manifest| manifest.covered_since),
            ) {
                (Some(source_since), Some(promoted_since)) => Some(source_since > promoted_since),
                _ => None,
            },
            Some(false) | None => None,
        };
        let source_contract_currently_excludes_older_rows = match (
            source_window_matches_current_bounded_contract,
            same_source_db_as_promoted,
            source_bounded_probe.covered_since,
            promoted_manifest,
        ) {
            (Some(true), Some(true), Some(source_since), Some(promoted_manifest)) => {
                match (Some(source_since), promoted_manifest.covered_since) {
                    (Some(source_since), Some(promoted_since)) => {
                        Some(source_since > promoted_since)
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        let promoted_reflects_older_still_promoted_window =
            match (same_source_db_as_promoted, source_start_later_than_promoted) {
                (Some(true), Some(value)) => Some(value),
                _ => None,
            };
        let source_prune_activity_recorded =
            source_state.map(|state| state.last_pruned_rows > 0 && state.last_pruned_at.is_some());

        let (reason_class, observed, basis, explanation, basis_explanation) =
            Self::classify_recent_raw_source_window_contract(
                state.promoted_exists,
                state.promoted.manifest_error.as_deref(),
                current_source_state_available,
                same_source_db_as_promoted,
                source_window_matches_current_bounded_contract,
                source_start_matches_promoted_start,
                source_start_later_than_promoted,
                source_contract_currently_excludes_older_rows,
                promoted_reflects_older_still_promoted_window,
                source_prune_activity_recorded,
                source_scan_error.as_deref(),
            );

        Ok(RecentRawSourceWindowContractDiagnostic {
            recent_raw_snapshot_dir: state.snapshot_dir.display().to_string(),
            recent_raw_source_window_contract_observed: observed,
            recent_raw_source_window_contract_reason_class: reason_class,
            recent_raw_source_window_contract_explanation: explanation,
            recent_raw_source_window_probe_bounded: true,
            recent_raw_source_window_probe_mode: RecentRawSourceWindowProbeMode::BoundedIndexEdges,
            recent_raw_source_window_probe_explanation: "default source-window-contract mode validates only the oldest and newest retained observed_swaps rows via the ts/slot/signature index, does not perform a full observed_swaps row-count scan, and leaves deep-scan-only scanned_* fields null".to_string(),
            recent_raw_promoted_covered_since: promoted_manifest
                .and_then(|manifest| manifest.covered_since),
            recent_raw_source_covered_since: source_state.and_then(|state| state.covered_since),
            recent_raw_source_bounded_probe_covered_since: source_bounded_probe.covered_since,
            recent_raw_source_scanned_covered_since: None,
            recent_raw_promoted_covered_through: promoted_manifest
                .and_then(|manifest| manifest.covered_through_cursor.clone()),
            recent_raw_source_covered_through: source_state
                .and_then(|state| state.covered_through_cursor.clone()),
            recent_raw_source_bounded_probe_covered_through:
                source_bounded_probe.covered_through_cursor,
            recent_raw_source_scanned_covered_through: None,
            recent_raw_source_start_later_than_promoted: source_start_later_than_promoted,
            recent_raw_source_contract_currently_excludes_older_rows:
                source_contract_currently_excludes_older_rows,
            recent_raw_source_window_matches_current_bounded_contract:
                source_window_matches_current_bounded_contract,
            recent_raw_promoted_reflects_older_still_promoted_window:
                promoted_reflects_older_still_promoted_window,
            recent_raw_source_window_contract_basis: basis,
            recent_raw_source_window_contract_basis_explanation: basis_explanation,
            recent_raw_source_same_source_db_as_promoted: same_source_db_as_promoted,
            recent_raw_source_cached_state_matches_bounded_probe:
                source_cached_state_matches_bounded_probe,
            recent_raw_source_cached_state_matches_scanned_rows: None,
            recent_raw_source_row_count: source_state.map(|state| state.row_count),
            recent_raw_source_scanned_row_count: None,
            recent_raw_source_last_pruned_rows: source_state.map(|state| state.last_pruned_rows),
            recent_raw_source_last_pruned_at: source_state.and_then(|state| state.last_pruned_at),
            recent_raw_source_prune_activity_recorded: source_prune_activity_recorded,
        })
    }
}
