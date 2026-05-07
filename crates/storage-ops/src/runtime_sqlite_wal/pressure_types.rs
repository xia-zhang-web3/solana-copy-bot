use chrono::{DateTime, Utc};
use serde::Serialize;
use std::path::PathBuf;

pub(super) const USAGE: &str = "usage: copybot_runtime_sqlite_wal_pressure_report --config <path> [--db-path <path>] --json [--large-wal-threshold-bytes <n>] [--critical-wal-threshold-bytes <n>]";

pub(super) const DEFAULT_LARGE_WAL_THRESHOLD_BYTES: u64 = 1_073_741_824;
pub(super) const DEFAULT_CRITICAL_WAL_THRESHOLD_BYTES: u64 = 8_589_934_592;

pub(super) const REASON_NONE: &str = "runtime_sqlite_wal_pressure_none";
pub(super) const REASON_LARGE: &str = "runtime_sqlite_wal_pressure_large";
pub(super) const REASON_CRITICAL: &str = "runtime_sqlite_wal_pressure_critical";
pub(super) const REASON_UNPROVEN_METADATA: &str = "runtime_sqlite_wal_pressure_unproven_metadata";

pub(super) const ACTION_NONE: &str = "no WAL pressure action";
pub(super) const ACTION_LARGE: &str = "continue monitoring or schedule bounded maintenance window";
pub(super) const ACTION_CRITICAL: &str =
    "stop service during low-risk window, run SQLite-managed checkpoint/truncate as copybot, restart, verify tails";
pub(super) const ACTION_UNPROVEN: &str =
    "prove runtime SQLite file metadata before choosing a maintenance action";

#[derive(Debug, Clone)]
pub(super) struct Cli {
    pub(super) config_path: PathBuf,
    pub(super) db_path_override: Option<PathBuf>,
    pub(super) json: bool,
    pub(super) large_wal_threshold_bytes: u64,
    pub(super) critical_wal_threshold_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum WalPressureLevel {
    None,
    Large,
    Critical,
    Unproven,
}

impl WalPressureLevel {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Large => "large",
            Self::Critical => "critical",
            Self::Unproven => "unproven",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct RuntimeSqliteWalPressureReport {
    pub(super) production_green: bool,
    pub(super) runtime_db_path: Option<String>,
    pub(super) db_bytes: Option<u64>,
    pub(super) wal_bytes: Option<u64>,
    pub(super) shm_bytes: Option<u64>,
    pub(super) wal_exists: Option<bool>,
    pub(super) shm_exists: Option<bool>,
    pub(super) db_mtime_utc: Option<DateTime<Utc>>,
    pub(super) wal_mtime_utc: Option<DateTime<Utc>>,
    pub(super) shm_mtime_utc: Option<DateTime<Utc>>,
    pub(super) large_wal_threshold_bytes: u64,
    pub(super) critical_wal_threshold_bytes: u64,
    pub(super) wal_pressure_level: WalPressureLevel,
    pub(super) wal_pressure_reason: String,
    pub(super) service_safe_next_action: String,
    pub(super) manual_operator_action_required: bool,
    pub(super) metadata_error: Option<String>,
}

impl RuntimeSqliteWalPressureReport {
    pub(super) fn exit_code(&self) -> i32 {
        match self.wal_pressure_level {
            WalPressureLevel::Unproven => 1,
            WalPressureLevel::None | WalPressureLevel::Large | WalPressureLevel::Critical => 0,
        }
    }
}
