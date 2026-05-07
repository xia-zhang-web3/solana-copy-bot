use crate::*;

impl DiscoveryService {
    pub(crate) fn load_recent_raw_diagnostic_state_read_only(
        state_root: &Path,
    ) -> RecentRawDiagnosticState {
        let snapshot_dir = Self::recent_raw_snapshot_dir_for_state_root(state_root);
        let promoted_snapshot_path = runtime_artifacts::journal_snapshot_latest_path(&snapshot_dir);
        let promoted_metadata_path =
            runtime_artifacts::journal_snapshot_latest_metadata_path(&snapshot_dir);
        let staged_snapshot_path = Self::recent_raw_staged_snapshot_path(&snapshot_dir);
        let staged_metadata_path = Self::recent_raw_staged_metadata_path(&snapshot_dir);
        let staged_candidates = Self::recent_raw_staged_candidate_reads(&snapshot_dir);

        let promoted = Self::read_recent_raw_surface_manifest(
            &promoted_snapshot_path,
            &promoted_metadata_path,
        );
        let staged =
            Self::read_recent_raw_surface_manifest(&staged_snapshot_path, &staged_metadata_path);
        let promoted_exists = promoted.manifest.is_some();
        let staged_exists = staged.manifest.is_some();

        let preferred_source_path = staged
            .manifest
            .as_ref()
            .map(|manifest| PathBuf::from(&manifest.source_db_path))
            .or_else(|| {
                promoted
                    .manifest
                    .as_ref()
                    .map(|manifest| PathBuf::from(&manifest.source_db_path))
            });

        let (
            runtime_db_path,
            runtime_db_size_bytes,
            runtime_db_mtime,
            runtime_db_wal_present,
            runtime_db_wal_size_bytes,
        ) = if let Some(path) = preferred_source_path.as_ref() {
            let (size_bytes, mtime, wal_present, wal_size_bytes) =
                Self::recent_raw_runtime_db_file_metadata(path);
            (
                Some(path.display().to_string()),
                size_bytes,
                mtime,
                wal_present,
                wal_size_bytes,
            )
        } else {
            (None, None, None, false, None)
        };

        let source_state = preferred_source_path
            .as_ref()
            .and_then(|path| Self::load_recent_raw_source_state_read_only(path).ok());
        let source_state_available = source_state.is_some();
        let source_outruns_promoted = source_state.as_ref().and_then(|source_state| {
            promoted
                .manifest
                .as_ref()
                .map(|manifest| Self::recent_raw_source_outruns_manifest(source_state, manifest))
        });
        let source_outruns_staged = source_state.as_ref().and_then(|source_state| {
            staged
                .manifest
                .as_ref()
                .map(|manifest| Self::recent_raw_source_outruns_manifest(source_state, manifest))
        });
        let staged_vs_promoted_relation =
            match (staged.manifest.as_ref(), promoted.manifest.as_ref()) {
                (Some(staged_manifest), Some(promoted_manifest)) => {
                    Self::recent_raw_manifest_progress_relation(staged_manifest, promoted_manifest)
                }
                _ => None,
            };

        RecentRawDiagnosticState {
            snapshot_dir,
            promoted_snapshot_path,
            promoted_metadata_path,
            staged_snapshot_path,
            staged_metadata_path,
            staged_candidates,
            promoted,
            staged,
            promoted_exists,
            staged_exists,
            runtime_db_path,
            runtime_db_size_bytes,
            runtime_db_mtime,
            runtime_db_wal_present,
            runtime_db_wal_size_bytes,
            source_state,
            source_state_available,
            source_outruns_promoted,
            source_outruns_staged,
            staged_vs_promoted_relation,
        }
    }
}
