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
pub const GAP_FILL_ARCHIVE_PREFIX: &str = "discovery_raw_gap_fill_";
pub const GAP_FILL_ARCHIVE_SUFFIX: &str = ".sqlite";
pub const GAP_FILL_LATEST_DB_NAME: &str = "latest.sqlite";
pub const GAP_FILL_LATEST_METADATA_NAME: &str = "latest.json";
pub const GAP_FILL_HELIUS_ARCHIVE_PREFIX: &str = "discovery_raw_gap_fill_helius_";
pub const GAP_FILL_HELIUS_ARCHIVE_SUFFIX: &str = ".sqlite";
pub const GAP_FILL_HELIUS_LATEST_DB_NAME: &str = "latest.sqlite";
pub const GAP_FILL_HELIUS_LATEST_METADATA_NAME: &str = "latest.json";
pub const PROGRAM_HISTORY_GAP_FILL_ARCHIVE_PREFIX: &str = "discovery_raw_gap_fill_program_history_";
pub const PROGRAM_HISTORY_GAP_FILL_ARCHIVE_SUFFIX: &str = ".sqlite";
pub const PROGRAM_HISTORY_GAP_FILL_LATEST_DB_NAME: &str = "latest.sqlite";
pub const PROGRAM_HISTORY_GAP_FILL_LATEST_METADATA_NAME: &str = "latest.json";

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

pub fn gap_fill_archive_path(dir: &Path, now: DateTime<Utc>) -> PathBuf {
    dir.join(format!(
        "{GAP_FILL_ARCHIVE_PREFIX}{}{GAP_FILL_ARCHIVE_SUFFIX}",
        timestamp_slug(now)
    ))
}

pub fn gap_fill_latest_path(dir: &Path) -> PathBuf {
    dir.join(GAP_FILL_LATEST_DB_NAME)
}

pub fn gap_fill_latest_metadata_path(dir: &Path) -> PathBuf {
    dir.join(GAP_FILL_LATEST_METADATA_NAME)
}

pub fn gap_fill_helius_archive_path(dir: &Path, now: DateTime<Utc>) -> PathBuf {
    dir.join(format!(
        "{GAP_FILL_HELIUS_ARCHIVE_PREFIX}{}{GAP_FILL_HELIUS_ARCHIVE_SUFFIX}",
        timestamp_slug(now)
    ))
}

pub fn gap_fill_helius_latest_path(dir: &Path) -> PathBuf {
    dir.join(GAP_FILL_HELIUS_LATEST_DB_NAME)
}

pub fn gap_fill_helius_latest_metadata_path(dir: &Path) -> PathBuf {
    dir.join(GAP_FILL_HELIUS_LATEST_METADATA_NAME)
}

pub fn program_history_gap_fill_archive_path(dir: &Path, now: DateTime<Utc>) -> PathBuf {
    dir.join(format!(
        "{PROGRAM_HISTORY_GAP_FILL_ARCHIVE_PREFIX}{}{PROGRAM_HISTORY_GAP_FILL_ARCHIVE_SUFFIX}",
        timestamp_slug(now)
    ))
}

pub fn program_history_gap_fill_latest_path(dir: &Path) -> PathBuf {
    dir.join(PROGRAM_HISTORY_GAP_FILL_LATEST_DB_NAME)
}

pub fn program_history_gap_fill_latest_metadata_path(dir: &Path) -> PathBuf {
    dir.join(PROGRAM_HISTORY_GAP_FILL_LATEST_METADATA_NAME)
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
mod tests {
    use super::{
        artifact_archive_path, copy_atomic, gap_fill_archive_path, gap_fill_helius_archive_path,
        journal_snapshot_archive_path, load_json, program_history_gap_fill_archive_path,
        prune_rotated_archives, resolve_relative_to_config, timestamp_slug, write_json_atomic,
    };
    use anyhow::Result;
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};
    use std::fs;
    use std::path::Path;
    use tempfile::tempdir;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct SampleJson {
        value: String,
    }

    #[test]
    fn resolve_relative_to_config_anchors_paths_to_config_parent() {
        let resolved = resolve_relative_to_config(
            Path::new("/tmp/config/live.toml"),
            Path::new("state/discovery_restore"),
        );
        assert_eq!(resolved, Path::new("/tmp/config/state/discovery_restore"));
    }

    #[test]
    fn timestamp_slug_is_compact_and_sortable() -> Result<()> {
        let now = DateTime::parse_from_rfc3339("2026-03-24T13:30:00Z")?.with_timezone(&Utc);
        assert_eq!(timestamp_slug(now), "20260324T133000Z");
        assert_eq!(
            artifact_archive_path(Path::new("/tmp"), now),
            Path::new("/tmp/discovery_runtime_20260324T133000Z.json")
        );
        assert_eq!(
            journal_snapshot_archive_path(Path::new("/tmp"), now),
            Path::new("/tmp/discovery_recent_raw_20260324T133000Z.sqlite")
        );
        assert_eq!(
            gap_fill_archive_path(Path::new("/tmp"), now),
            Path::new("/tmp/discovery_raw_gap_fill_20260324T133000Z.sqlite")
        );
        assert_eq!(
            gap_fill_helius_archive_path(Path::new("/tmp"), now),
            Path::new("/tmp/discovery_raw_gap_fill_helius_20260324T133000Z.sqlite")
        );
        assert_eq!(
            program_history_gap_fill_archive_path(Path::new("/tmp"), now),
            Path::new("/tmp/discovery_raw_gap_fill_program_history_20260324T133000Z.sqlite")
        );
        Ok(())
    }

    #[test]
    fn prune_rotated_archives_keeps_newest_entries() -> Result<()> {
        let temp = tempdir()?;
        for suffix in ["20260324T120000Z", "20260324T121000Z", "20260324T122000Z"] {
            fs::write(
                temp.path().join(format!("discovery_runtime_{suffix}.json")),
                suffix,
            )?;
        }
        let pruned = prune_rotated_archives(temp.path(), "discovery_runtime_", ".json", 2)?;
        assert_eq!(pruned.len(), 1);
        assert_eq!(
            pruned[0].file_name().and_then(|name| name.to_str()),
            Some("discovery_runtime_20260324T120000Z.json")
        );
        Ok(())
    }

    #[test]
    fn write_json_atomic_and_copy_atomic_preserve_payload() -> Result<()> {
        let temp = tempdir()?;
        let source = temp.path().join("source.json");
        let latest = temp.path().join("latest.json");
        write_json_atomic(
            &source,
            &SampleJson {
                value: "payload".to_string(),
            },
        )?;
        copy_atomic(&source, &latest)?;
        let latest_json: SampleJson = load_json(&latest)?;
        assert_eq!(
            latest_json,
            SampleJson {
                value: "payload".to_string()
            }
        );
        Ok(())
    }
}
