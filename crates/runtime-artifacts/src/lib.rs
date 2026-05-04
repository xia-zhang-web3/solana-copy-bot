use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const ARTIFACT_ARCHIVE_PREFIX: &str = "discovery_runtime_";
pub const ARTIFACT_ARCHIVE_SUFFIX: &str = ".json";
pub const ARTIFACT_LATEST_FILE_NAME: &str = "latest.json";
pub const JOURNAL_SNAPSHOT_ARCHIVE_PREFIX: &str = "discovery_recent_raw_";
pub const JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX: &str = ".sqlite";
pub const JOURNAL_METADATA_SUFFIX: &str = ".json";
pub const JOURNAL_LATEST_DB_NAME: &str = "latest.sqlite";
pub const JOURNAL_LATEST_METADATA_NAME: &str = "latest.json";

pub fn resolve_relative_to_config(config_path: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(path)
}

pub fn resolve_db_path(
    config_path: &Path,
    db_path_override: Option<&Path>,
    configured_db_path: &str,
) -> PathBuf {
    if let Some(db_path_override) = db_path_override {
        return db_path_override.to_path_buf();
    }
    resolve_relative_to_config(config_path, Path::new(configured_db_path))
}

pub fn resolve_migrations_dir(config_path: &Path, configured_migrations_dir: &str) -> PathBuf {
    let configured = PathBuf::from(configured_migrations_dir);
    if configured.is_absolute() || configured.exists() {
        return configured;
    }
    if let Some(config_parent) = config_path.parent() {
        let sibling_candidate = config_parent.join(&configured);
        if sibling_candidate.exists() {
            return sibling_candidate;
        }
        if let Some(project_root) = config_parent.parent() {
            let root_candidate = project_root.join(&configured);
            if root_candidate.exists() {
                return root_candidate;
            }
        }
    }
    let repo_fallback = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    if repo_fallback.exists() {
        return repo_fallback;
    }
    configured
}

pub fn timestamp_slug(now: DateTime<Utc>) -> String {
    now.format("%Y%m%dT%H%M%SZ").to_string()
}

pub fn artifact_archive_path(dir: &Path, now: DateTime<Utc>) -> PathBuf {
    dir.join(format!(
        "{ARTIFACT_ARCHIVE_PREFIX}{}{ARTIFACT_ARCHIVE_SUFFIX}",
        timestamp_slug(now)
    ))
}

pub fn artifact_latest_path(dir: &Path) -> PathBuf {
    dir.join(ARTIFACT_LATEST_FILE_NAME)
}

pub fn journal_snapshot_archive_path(dir: &Path, now: DateTime<Utc>) -> PathBuf {
    dir.join(format!(
        "{JOURNAL_SNAPSHOT_ARCHIVE_PREFIX}{}{JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX}",
        timestamp_slug(now)
    ))
}

pub fn journal_snapshot_metadata_path(snapshot_path: &Path) -> PathBuf {
    snapshot_path.with_extension("json")
}

pub fn journal_snapshot_latest_path(dir: &Path) -> PathBuf {
    dir.join(JOURNAL_LATEST_DB_NAME)
}

pub fn journal_snapshot_latest_metadata_path(dir: &Path) -> PathBuf {
    dir.join(JOURNAL_LATEST_METADATA_NAME)
}

pub fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating {}", parent.display()))?;
    }
    Ok(())
}

pub fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)
        .with_context(|| format!("failed serializing json for {}", path.display()))?;
    write_bytes_atomic(path, &bytes)
}

pub fn load_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_slice(
        &fs::read(path).with_context(|| format!("failed reading {}", path.display()))?,
    )
    .with_context(|| format!("failed parsing json {}", path.display()))
}

pub fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    ensure_parent_dir(path)?;
    let temp_path = temp_path(path);
    fs::write(&temp_path, bytes)
        .with_context(|| format!("failed writing {}", temp_path.display()))?;
    fs::rename(&temp_path, path).with_context(|| {
        format!(
            "failed renaming {} to {}",
            temp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

pub fn copy_atomic(source_path: &Path, destination_path: &Path) -> Result<()> {
    ensure_parent_dir(destination_path)?;
    let temp_path = temp_path(destination_path);
    fs::copy(source_path, &temp_path).with_context(|| {
        format!(
            "failed copying {} to {}",
            source_path.display(),
            temp_path.display()
        )
    })?;
    fs::rename(&temp_path, destination_path).with_context(|| {
        format!(
            "failed renaming {} to {}",
            temp_path.display(),
            destination_path.display()
        )
    })?;
    Ok(())
}

pub fn prune_rotated_archives(
    dir: &Path,
    prefix: &str,
    suffix: &str,
    keep: usize,
) -> Result<Vec<PathBuf>> {
    let mut entries = Vec::new();
    if !dir.exists() {
        return Ok(entries);
    }
    for entry in fs::read_dir(dir).with_context(|| format!("failed reading {}", dir.display()))? {
        let entry = entry.with_context(|| format!("failed reading {}", dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with(prefix) && name.ends_with(suffix) {
            entries.push(path);
        }
    }
    entries.sort_by(|left, right| right.file_name().cmp(&left.file_name()));
    let mut pruned = Vec::new();
    for path in entries.into_iter().skip(keep) {
        fs::remove_file(&path).with_context(|| format!("failed removing {}", path.display()))?;
        pruned.push(path);
    }
    Ok(pruned)
}

fn temp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("tmp");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.with_file_name(format!(".{file_name}.tmp-{}-{nonce}", std::process::id()))
}

#[cfg(test)]
mod tests;
