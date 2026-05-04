const USAGE: &str = "usage: copybot_runtime_sqlite_wal_maintenance --config <path> [--db-path <path>] [--service-name <name>] --json [--min-wal-bytes <n>] [--critical-wal-bytes <n>] [--timeout-seconds <n>] [--dry-run] [--allow-service-active]";

const DEFAULT_SERVICE_NAME: &str = "solana-copy-bot.service";
const DEFAULT_MIN_WAL_BYTES: u64 = 1_073_741_824;
const DEFAULT_CRITICAL_WAL_BYTES: u64 = 8_589_934_592;
const DEFAULT_TIMEOUT_SECONDS: u64 = 900;

const OUTCOME_SKIPPED_NOT_NEEDED: &str = "skipped_not_needed";
const OUTCOME_SKIPPED_DRY_RUN: &str = "skipped_dry_run";
const OUTCOME_FAILED_SERVICE_ACTIVE: &str = "failed_service_active";
const OUTCOME_FAILED_CHECKPOINT_BUSY: &str = "failed_checkpoint_busy";
const OUTCOME_FAILED_TIMEOUT: &str = "failed_timeout";
const OUTCOME_FAILED_UNPROVEN: &str = "failed_unproven";
const OUTCOME_COMPLETED: &str = "completed";

const REASON_SERVICE_ACTIVE: &str = "runtime_sqlite_wal_maintenance_service_active";
const REASON_SERVICE_NOT_INACTIVE: &str = "runtime_sqlite_wal_maintenance_service_not_inactive";
const REASON_NOT_NEEDED: &str = "runtime_sqlite_wal_maintenance_not_needed";
const REASON_DRY_RUN: &str = "runtime_sqlite_wal_maintenance_dry_run";
const REASON_COMPLETED: &str = "runtime_sqlite_wal_maintenance_completed";
const REASON_CHECKPOINT_BUSY: &str = "runtime_sqlite_wal_maintenance_checkpoint_busy";
const REASON_TIMEOUT: &str = "runtime_sqlite_wal_maintenance_checkpoint_timeout";
const REASON_UNPROVEN: &str = "runtime_sqlite_wal_maintenance_unproven";

const ACTION_SERVICE_ACTIVE: &str =
    "stop service before runtime SQLite WAL maintenance, then rerun this operator";
const ACTION_NOT_NEEDED: &str = "no WAL maintenance action";
const ACTION_DRY_RUN: &str =
    "manual_operator_action_required: stop service and rerun without --dry-run to execute SQLite-managed checkpoint/truncate";
const ACTION_COMPLETED: &str = "restart service and verify runtime/recent_raw tails";
const ACTION_BUSY: &str =
    "confirm service and other SQLite users are stopped, then retry WAL maintenance";
const ACTION_TIMEOUT: &str =
    "retry during lower I/O pressure window or increase the explicit maintenance timeout";
const ACTION_UNPROVEN: &str = "prove service state and runtime SQLite metadata before maintenance";

#[derive(Debug, Clone)]
struct Cli {
    config_path: PathBuf,
    db_path_override: Option<PathBuf>,
    service_name: String,
    json: bool,
    min_wal_bytes: u64,
    critical_wal_bytes: u64,
    timeout_seconds: u64,
    dry_run: bool,
    allow_service_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServiceState {
    active_state: String,
    active: bool,
    substate: Option<String>,
}

impl ServiceState {
    fn service_inactive_for_maintenance(&self) -> bool {
        self.active_state == "inactive"
    }

    fn maintenance_block_reason(&self) -> &'static str {
        if self.active_state == "active" {
            REASON_SERVICE_ACTIVE
        } else {
            REASON_SERVICE_NOT_INACTIVE
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeSqliteWalMaintenanceReport {
    production_green: bool,
    dry_run: bool,
    service_name: String,
    service_active_state: Option<String>,
    service_active: Option<bool>,
    service_substate: Option<String>,
    runtime_db_path: Option<String>,
    before_db_bytes: Option<u64>,
    before_wal_bytes: Option<u64>,
    before_shm_bytes: Option<u64>,
    after_db_bytes: Option<u64>,
    after_wal_bytes: Option<u64>,
    after_shm_bytes: Option<u64>,
    min_wal_bytes: u64,
    critical_wal_bytes: u64,
    checkpoint_attempted: bool,
    checkpoint_busy: Option<i64>,
    checkpoint_log_frames: Option<i64>,
    checkpoint_checkpointed_frames: Option<i64>,
    maintenance_outcome: String,
    reason: String,
    final_wal_pressure_level: String,
    service_safe_next_action: String,
    error: Option<String>,
}

impl RuntimeSqliteWalMaintenanceReport {
    fn exit_code(&self) -> i32 {
        match self.maintenance_outcome.as_str() {
            OUTCOME_COMPLETED | OUTCOME_SKIPPED_NOT_NEEDED | OUTCOME_SKIPPED_DRY_RUN => 0,
            _ => 1,
        }
    }
}

impl Cli {
    fn default_for_error() -> Self {
        Self {
            config_path: PathBuf::new(),
            db_path_override: None,
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            json: true,
            min_wal_bytes: DEFAULT_MIN_WAL_BYTES,
            critical_wal_bytes: DEFAULT_CRITICAL_WAL_BYTES,
            timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
            dry_run: false,
            allow_service_active: false,
        }
    }
}

fn parse_args_from<I>(args: I) -> Result<Option<Cli>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut db_path_override: Option<PathBuf> = None;
    let mut service_name = DEFAULT_SERVICE_NAME.to_string();
    let mut json = false;
    let mut min_wal_bytes = DEFAULT_MIN_WAL_BYTES;
    let mut critical_wal_bytes = DEFAULT_CRITICAL_WAL_BYTES;
    let mut timeout_seconds = DEFAULT_TIMEOUT_SECONDS;
    let mut dry_run = false;
    let mut allow_service_active = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?));
            }
            "--db-path" => {
                db_path_override = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?));
            }
            "--service-name" => {
                service_name = parse_string_arg("--service-name", args.next())?;
            }
            "--json" => json = true,
            "--min-wal-bytes" => {
                min_wal_bytes = parse_u64_arg("--min-wal-bytes", args.next())?;
            }
            "--critical-wal-bytes" => {
                critical_wal_bytes = parse_u64_arg("--critical-wal-bytes", args.next())?;
            }
            "--timeout-seconds" => {
                timeout_seconds = parse_u64_arg("--timeout-seconds", args.next())?;
            }
            "--dry-run" => dry_run = true,
            "--allow-service-active" => allow_service_active = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Cli {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path_override,
        service_name,
        json,
        min_wal_bytes,
        critical_wal_bytes,
        timeout_seconds,
        dry_run,
        allow_service_active,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed.to_string())
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("{flag} must be an unsigned integer; got {raw}"))
}

fn validate_cli(cli: &Cli) -> Result<()> {
    if cli.min_wal_bytes == 0 {
        bail!("--min-wal-bytes must be greater than zero");
    }
    if cli.critical_wal_bytes < cli.min_wal_bytes {
        bail!("--critical-wal-bytes must be greater than or equal to --min-wal-bytes");
    }
    if cli.timeout_seconds == 0 {
        bail!("--timeout-seconds must be greater than zero");
    }
    Ok(())
}

fn resolve_runtime_db_path(cli: &Cli) -> Result<PathBuf> {
    if let Some(path) = &cli.db_path_override {
        return Ok(path.to_path_buf());
    }
    let loaded_config = load_from_path(&cli.config_path)
        .with_context(|| format!("failed loading config {}", cli.config_path.display()))?;
    Ok(resolve_db_path(
        &cli.config_path,
        &loaded_config.sqlite.path,
    ))
}
