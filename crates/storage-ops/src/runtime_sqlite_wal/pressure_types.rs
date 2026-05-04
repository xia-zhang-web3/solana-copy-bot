const USAGE: &str = "usage: copybot_runtime_sqlite_wal_pressure_report --config <path> [--db-path <path>] --json [--large-wal-threshold-bytes <n>] [--critical-wal-threshold-bytes <n>]";

const DEFAULT_LARGE_WAL_THRESHOLD_BYTES: u64 = 1_073_741_824;
const DEFAULT_CRITICAL_WAL_THRESHOLD_BYTES: u64 = 8_589_934_592;

const REASON_NONE: &str = "runtime_sqlite_wal_pressure_none";
const REASON_LARGE: &str = "runtime_sqlite_wal_pressure_large";
const REASON_CRITICAL: &str = "runtime_sqlite_wal_pressure_critical";
const REASON_UNPROVEN_METADATA: &str = "runtime_sqlite_wal_pressure_unproven_metadata";

const ACTION_NONE: &str = "no WAL pressure action";
const ACTION_LARGE: &str = "continue monitoring or schedule bounded maintenance window";
const ACTION_CRITICAL: &str =
    "stop service during low-risk window, run SQLite-managed checkpoint/truncate as copybot, restart, verify tails";
const ACTION_UNPROVEN: &str =
    "prove runtime SQLite file metadata before choosing a maintenance action";

#[derive(Debug, Clone)]
struct Cli {
    config_path: PathBuf,
    db_path_override: Option<PathBuf>,
    json: bool,
    large_wal_threshold_bytes: u64,
    critical_wal_threshold_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum WalPressureLevel {
    None,
    Large,
    Critical,
    Unproven,
}

impl WalPressureLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Large => "large",
            Self::Critical => "critical",
            Self::Unproven => "unproven",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeSqliteWalPressureReport {
    production_green: bool,
    runtime_db_path: Option<String>,
    db_bytes: Option<u64>,
    wal_bytes: Option<u64>,
    shm_bytes: Option<u64>,
    wal_exists: Option<bool>,
    shm_exists: Option<bool>,
    db_mtime_utc: Option<DateTime<Utc>>,
    wal_mtime_utc: Option<DateTime<Utc>>,
    shm_mtime_utc: Option<DateTime<Utc>>,
    large_wal_threshold_bytes: u64,
    critical_wal_threshold_bytes: u64,
    wal_pressure_level: WalPressureLevel,
    wal_pressure_reason: String,
    service_safe_next_action: String,
    manual_operator_action_required: bool,
    metadata_error: Option<String>,
}

impl RuntimeSqliteWalPressureReport {
    fn exit_code(&self) -> i32 {
        match self.wal_pressure_level {
            WalPressureLevel::Unproven => 1,
            WalPressureLevel::None | WalPressureLevel::Large | WalPressureLevel::Critical => 0,
        }
    }
}
