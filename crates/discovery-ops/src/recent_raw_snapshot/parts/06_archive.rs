pub(crate) use super::*;

#[path = "06_archive_manifest.rs"]
mod archive_manifest;
#[path = "06_archive_paths.rs"]
mod archive_paths;
#[path = "06_archive_progress.rs"]
mod archive_progress;
#[path = "06_archive_retention.rs"]
mod archive_retention;

pub(crate) use self::archive_manifest::{
    infer_created_at, load_required_cached_recent_raw_state, manifest_for_existing_snapshot,
    manifest_for_snapshot,
};
pub(crate) use self::archive_paths::{
    archive_candidate, load_existing_staged_manifest, remove_file_if_exists, remove_snapshot_set,
    remove_staged_snapshot_artifacts, sqlite_snapshot_shm_path, sqlite_snapshot_wal_path,
    staged_snapshot_archive_path, staged_snapshot_metadata_path,
};
pub(crate) use self::archive_progress::{
    latest_surface_can_seed_staged_progress, seed_staged_snapshot_from_latest_surface,
    source_contract_no_longer_matches_staged_progress, staged_manifest_for_state,
};
#[cfg(test)]
pub(crate) use self::archive_progress::{
    reference_surface_outran_staged_progress, source_window_outran_staged_progress,
};
pub(crate) use self::archive_retention::{
    enforce_snapshot_archive_retention, list_archive_snapshot_paths,
};
