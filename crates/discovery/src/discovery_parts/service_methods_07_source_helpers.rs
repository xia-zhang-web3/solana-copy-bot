use super::*;

impl DiscoveryService {
    pub(super) fn recent_raw_source_contract_no_longer_matches_manifest(
        source_state: &RecentRawJournalStateRow,
        manifest: &RecentRawPromotionSnapshotManifest,
    ) -> bool {
        if source_state.row_count < manifest.row_count {
            return true;
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
            return Self::runtime_cursor_cmp(source_cursor, manifest_cursor) == Ordering::Less;
        }
        false
    }

    pub(super) fn recent_raw_published_latest_supersedes_staged_progress(
        latest_manifest: &RecentRawPromotionSnapshotManifest,
        staged_manifest: &RecentRawPromotionSnapshotManifest,
    ) -> bool {
        latest_manifest.source_db_path == staged_manifest.source_db_path
            && Self::recent_raw_manifest_outruns_reference(latest_manifest, staged_manifest)
    }

    pub(super) fn recent_raw_latest_surface_can_seed_staged_progress(
        source_db_path: &Path,
        source_state: &RecentRawJournalStateRow,
        latest_manifest: &RecentRawPromotionSnapshotManifest,
        staged_manifest: Option<&RecentRawPromotionSnapshotManifest>,
    ) -> bool {
        latest_manifest.source_db_path == source_db_path.display().to_string()
            && latest_manifest.row_count > 0
            && !Self::recent_raw_source_contract_no_longer_matches_manifest(
                source_state,
                latest_manifest,
            )
            && staged_manifest.map_or(true, |staged_manifest| {
                Self::recent_raw_published_latest_supersedes_staged_progress(
                    latest_manifest,
                    staged_manifest,
                )
            })
    }
}
