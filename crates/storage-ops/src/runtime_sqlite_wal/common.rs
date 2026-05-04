use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FileMetadataSnapshot {
    pub(super) exists: bool,
    pub(super) bytes: u64,
    pub(super) modified_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RuntimeSqliteFilesSnapshot {
    pub(super) db_path: PathBuf,
    pub(super) db: FileMetadataSnapshot,
    pub(super) wal: FileMetadataSnapshot,
    pub(super) shm: FileMetadataSnapshot,
}

pub(super) fn inspect_runtime_sqlite_files(db_path: &Path) -> Result<RuntimeSqliteFilesSnapshot> {
    let db = inspect_required_file(db_path)?;
    let wal = inspect_optional_file(&sqlite_sidecar_path(db_path, "wal"))?;
    let shm = inspect_optional_file(&sqlite_sidecar_path(db_path, "shm"))?;
    Ok(RuntimeSqliteFilesSnapshot {
        db_path: db_path.to_path_buf(),
        db,
        wal,
        shm,
    })
}

fn inspect_required_file(path: &Path) -> Result<FileMetadataSnapshot> {
    match fs::metadata(path) {
        Ok(metadata) => metadata_snapshot(&metadata),
        Err(error) => {
            Err(error).with_context(|| format!("failed reading metadata for {}", path.display()))
        }
    }
}

fn inspect_optional_file(path: &Path) -> Result<FileMetadataSnapshot> {
    match fs::metadata(path) {
        Ok(metadata) => metadata_snapshot(&metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(FileMetadataSnapshot {
            exists: false,
            bytes: 0,
            modified_utc: None,
        }),
        Err(error) => {
            Err(error).with_context(|| format!("failed reading metadata for {}", path.display()))
        }
    }
}

fn metadata_snapshot(metadata: &fs::Metadata) -> Result<FileMetadataSnapshot> {
    Ok(FileMetadataSnapshot {
        exists: true,
        bytes: metadata.len(),
        modified_utc: metadata.modified().ok().and_then(system_time_to_utc),
    })
}

fn system_time_to_utc(time: SystemTime) -> Option<DateTime<Utc>> {
    Some(DateTime::<Utc>::from(time))
}

pub(super) fn sqlite_sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = db_path.as_os_str().to_os_string();
    sidecar.push("-");
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

pub(super) fn resolve_db_path(config_path: &Path, sqlite_path: &str) -> PathBuf {
    let configured = Path::new(sqlite_path.trim());
    if configured.is_absolute() {
        return configured.to_path_buf();
    }
    match config_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(configured),
        _ => configured.to_path_buf(),
    }
}

pub(super) fn compact_error(error: anyhow::Error) -> String {
    format!("{error:#}").replace('\n', " ")
}
