use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FileSnapshot {
    pub(super) exists: bool,
    pub(super) bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RuntimeDbFiles {
    pub(super) db_path: PathBuf,
    pub(super) db: FileSnapshot,
    pub(super) wal: FileSnapshot,
    pub(super) shm: FileSnapshot,
}

pub(super) fn resolve_db_path(config_path: &Path, sqlite_path: &str) -> PathBuf {
    let configured = Path::new(sqlite_path.trim());
    if configured.is_absolute() {
        return configured.to_path_buf();
    }
    config_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(|parent| parent.join(configured))
        .unwrap_or_else(|| configured.to_path_buf())
}

pub(super) fn inspect_runtime_db_files(db_path: &Path) -> Result<RuntimeDbFiles> {
    Ok(RuntimeDbFiles {
        db_path: db_path.to_path_buf(),
        db: inspect_file(db_path, true)?,
        wal: inspect_file(&sqlite_sidecar_path(db_path, "wal"), false)?,
        shm: inspect_file(&sqlite_sidecar_path(db_path, "shm"), false)?,
    })
}

pub(super) fn sqlite_sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = db_path.as_os_str().to_os_string();
    sidecar.push("-");
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

pub(super) fn compact_error(error: anyhow::Error) -> String {
    format!("{error:#}").replace('\n', " ")
}

fn inspect_file(path: &Path, required: bool) -> Result<FileSnapshot> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(FileSnapshot {
            exists: true,
            bytes: metadata.len(),
        }),
        Err(error) if !required && error.kind() == std::io::ErrorKind::NotFound => {
            Ok(FileSnapshot {
                exists: false,
                bytes: 0,
            })
        }
        Err(error) => {
            Err(error).with_context(|| format!("failed reading metadata for {}", path.display()))
        }
    }
}

#[allow(dead_code)]
fn system_time_to_utc(time: SystemTime) -> DateTime<Utc> {
    DateTime::<Utc>::from(time)
}
