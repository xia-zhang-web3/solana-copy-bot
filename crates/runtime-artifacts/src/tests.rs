use super::{
    artifact_archive_path, copy_atomic, journal_snapshot_archive_path, load_json,
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
