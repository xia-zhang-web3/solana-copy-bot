use super::*;

impl DiscoveryService {
    pub(crate) fn read_recent_raw_surface_manifest(
        snapshot_path: &Path,
        metadata_path: &Path,
    ) -> RecentRawSurfaceRead {
        let snapshot_present = snapshot_path.exists();
        let metadata_present = metadata_path.exists();
        if !snapshot_present && !metadata_present {
            return RecentRawSurfaceRead::default();
        }
        if !snapshot_present || !metadata_present {
            return RecentRawSurfaceRead {
                snapshot_present,
                metadata_present,
                manifest: None,
                manifest_error: Some(format!(
                    "recent_raw surface is partial (snapshot_present={}, metadata_present={})",
                    snapshot_present, metadata_present
                )),
            };
        }
        match runtime_artifacts::load_json::<RecentRawPromotionSnapshotManifest>(metadata_path) {
            Ok(manifest) => RecentRawSurfaceRead {
                snapshot_present,
                metadata_present,
                manifest: Some(manifest),
                manifest_error: None,
            },
            Err(error) => RecentRawSurfaceRead {
                snapshot_present,
                metadata_present,
                manifest: None,
                manifest_error: Some(format!(
                    "failed parsing recent_raw metadata {}: {error:#}",
                    metadata_path.display()
                )),
            },
        }
    }

    pub(crate) fn read_recent_raw_snapshot_sqlite_content_read_only(
        snapshot_path: &Path,
    ) -> RecentRawSnapshotSqliteContentRead {
        if !snapshot_path.exists() {
            return RecentRawSnapshotSqliteContentRead::default();
        }
        match SqliteStore::open_read_only(snapshot_path)
            .and_then(|store| store.recent_raw_journal_state_cached_read_only_required())
        {
            Ok(state) => RecentRawSnapshotSqliteContentRead {
                state: Some(state),
                error: None,
            },
            Err(error) => RecentRawSnapshotSqliteContentRead {
                state: None,
                error: Some(format!(
                    "failed reading staged snapshot sqlite content {}: {error:#}",
                    snapshot_path.display()
                )),
            },
        }
    }

    pub(crate) fn recent_raw_source_outruns_manifest(
        source_state: &RecentRawJournalStateRow,
        manifest: &RecentRawPromotionSnapshotManifest,
    ) -> bool {
        if source_state.row_count == 0 {
            return manifest.row_count > 0;
        }
        if let (Some(source_since), Some(manifest_since)) =
            (source_state.covered_since, manifest.covered_since)
        {
            if source_since > manifest_since {
                return true;
            }
        }
        if let (Some(source_cursor), Some(manifest_cursor)) = (
            source_state.covered_through_cursor.as_ref(),
            manifest.covered_through_cursor.as_ref(),
        ) {
            return Self::runtime_cursor_cmp(source_cursor, manifest_cursor) == Ordering::Greater;
        }
        false
    }

    pub(crate) fn recent_raw_manifest_outruns_reference(
        candidate: &RecentRawPromotionSnapshotManifest,
        reference: &RecentRawPromotionSnapshotManifest,
    ) -> bool {
        if candidate.row_count == 0 {
            return reference.row_count > 0;
        }
        if candidate.row_count > reference.row_count {
            return true;
        }
        if let (Some(candidate_since), Some(reference_since)) =
            (candidate.covered_since, reference.covered_since)
        {
            if candidate_since > reference_since {
                return true;
            }
        }
        if let (Some(candidate_cursor), Some(reference_cursor)) = (
            candidate.covered_through_cursor.as_ref(),
            reference.covered_through_cursor.as_ref(),
        ) {
            return Self::runtime_cursor_cmp(candidate_cursor, reference_cursor)
                == Ordering::Greater;
        }
        false
    }

    pub(crate) fn recent_raw_manifest_progress_relation(
        candidate: &RecentRawPromotionSnapshotManifest,
        reference: &RecentRawPromotionSnapshotManifest,
    ) -> Option<RecentRawManifestProgressRelation> {
        if candidate.source_db_path != reference.source_db_path {
            return None;
        }
        let candidate_outruns_reference =
            Self::recent_raw_manifest_outruns_reference(candidate, reference);
        let reference_outruns_candidate =
            Self::recent_raw_manifest_outruns_reference(reference, candidate);
        Some(
            if candidate_outruns_reference && !reference_outruns_candidate {
                RecentRawManifestProgressRelation::CandidateAhead
            } else if reference_outruns_candidate && !candidate_outruns_reference {
                RecentRawManifestProgressRelation::ReferenceAhead
            } else {
                RecentRawManifestProgressRelation::Equivalent
            },
        )
    }

    pub(crate) fn recent_raw_optional_cursor_equal(
        left: Option<&DiscoveryRuntimeCursor>,
        right: Option<&DiscoveryRuntimeCursor>,
    ) -> bool {
        match (left, right) {
            (Some(left), Some(right)) => Self::runtime_cursor_cmp(left, right) == Ordering::Equal,
            (None, None) => true,
            _ => false,
        }
    }

    pub(crate) fn recent_raw_staged_selection_reason(
        staged_snapshot_path: &Path,
        staged_metadata_path: &Path,
        candidate_count: usize,
        selected_is_latest_candidate: Option<bool>,
        staged_exists: bool,
    ) -> String {
        if !staged_exists {
            return format!(
                "recent_raw diagnostics read the fixed staged artifact paths {} and {}, but no parseable staged manifest is currently available at that fixed selection point",
                staged_snapshot_path.display(),
                staged_metadata_path.display()
            );
        }
        match (candidate_count, selected_is_latest_candidate) {
            (count, Some(false)) if count > 1 => format!(
                "recent_raw diagnostics select the fixed staged artifact paths {} and {} directly; candidate scan found {} staged candidates, and the selected fixed staged artifact is not the newest parseable candidate by created_at",
                staged_snapshot_path.display(),
                staged_metadata_path.display(),
                count
            ),
            (count, Some(true)) if count > 1 => format!(
                "recent_raw diagnostics select the fixed staged artifact paths {} and {} directly; candidate scan found {} staged candidates, but the fixed selected staged artifact is also the newest parseable candidate by created_at",
                staged_snapshot_path.display(),
                staged_metadata_path.display(),
                count
            ),
            _ => format!(
                "recent_raw diagnostics select the fixed staged artifact paths {} and {} directly; no dynamic ranking across staged candidates is applied",
                staged_snapshot_path.display(),
                staged_metadata_path.display()
            ),
        }
    }

    pub(crate) fn recent_raw_runtime_db_file_metadata(
        runtime_db_path: &Path,
    ) -> (Option<u64>, Option<String>, bool, Option<u64>) {
        let runtime_db_metadata = fs::metadata(runtime_db_path).ok();
        let runtime_db_size_bytes = runtime_db_metadata.as_ref().map(fs::Metadata::len);
        let runtime_db_mtime = runtime_db_metadata
            .and_then(|metadata| metadata.modified().ok())
            .map(DateTime::<Utc>::from)
            .map(|mtime| mtime.to_rfc3339());
        let wal_path = PathBuf::from(format!("{}-wal", runtime_db_path.display()));
        let wal_metadata = fs::metadata(&wal_path).ok();
        let wal_present = wal_metadata.is_some();
        let wal_size_bytes = wal_metadata.map(|metadata| metadata.len());
        (
            runtime_db_size_bytes,
            runtime_db_mtime,
            wal_present,
            wal_size_bytes,
        )
    }

    pub(crate) fn parse_recent_raw_optional_rfc3339_utc(
        raw: Option<String>,
        field_name: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        raw.map(|raw| {
            DateTime::parse_from_rfc3339(&raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| format!("invalid {field_name} timestamp value: {raw}"))
        })
        .transpose()
    }
}
