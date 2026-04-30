use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use rusqlite::Connection;
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

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
struct FileMetadataSnapshot {
    exists: bool,
    bytes: u64,
    modified_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeSqliteFilesSnapshot {
    db_path: PathBuf,
    db: FileMetadataSnapshot,
    wal: FileMetadataSnapshot,
    shm: FileMetadataSnapshot,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CheckpointResult {
    busy: i64,
    log_frames: i64,
    checkpointed_frames: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CheckpointFailure {
    Timeout(String),
    Unproven(String),
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

fn main() {
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(Some(cli)) => run_with_hooks(
            &cli,
            load_service_state_from_systemctl,
            run_sqlite_wal_checkpoint_truncate,
        ),
        Ok(None) => {
            println!("{USAGE}");
            return;
        }
        Err(error) => failed_unproven_report(
            &Cli::default_for_error(),
            None,
            None,
            None,
            Some(compact_error(error)),
        ),
    };

    if report_is_json_requested(&report) {
        println!(
            "{}",
            serde_json::to_string(&report).expect("WAL maintenance report must serialize")
        );
    } else {
        println!(
            "maintenance_outcome={} reason={} production_green=false",
            report.maintenance_outcome, report.reason
        );
    }
    std::process::exit(report.exit_code());
}

fn report_is_json_requested(report: &RuntimeSqliteWalMaintenanceReport) -> bool {
    report.error.as_deref() != Some("runtime_sqlite_wal_maintenance_json_required")
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

fn run_with_hooks<ServiceProbe, Checkpoint>(
    cli: &Cli,
    service_probe: ServiceProbe,
    checkpoint: Checkpoint,
) -> RuntimeSqliteWalMaintenanceReport
where
    ServiceProbe: Fn(&str) -> Result<ServiceState>,
    Checkpoint: Fn(&Path, u64) -> Result<CheckpointResult, CheckpointFailure>,
{
    if !cli.json {
        return failed_unproven_report(
            cli,
            None,
            None,
            None,
            Some("runtime_sqlite_wal_maintenance_json_required".to_string()),
        );
    }
    if let Err(error) = validate_cli(cli) {
        return failed_unproven_report(cli, None, None, None, Some(compact_error(error)));
    }

    let db_path = match resolve_runtime_db_path(cli) {
        Ok(path) => path,
        Err(error) => {
            return failed_unproven_report(cli, None, None, None, Some(compact_error(error)));
        }
    };
    let before = match inspect_runtime_sqlite_files(&db_path) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return failed_unproven_report(
                cli,
                Some(db_path),
                None,
                None,
                Some(compact_error(error)),
            );
        }
    };
    let service = match service_probe(&cli.service_name) {
        Ok(service) => service,
        Err(error) => {
            return failed_unproven_report(
                cli,
                Some(db_path),
                Some(&before),
                None,
                Some(compact_error(error)),
            );
        }
    };

    if !service.service_inactive_for_maintenance() && !cli.allow_service_active {
        let reason = service.maintenance_block_reason();
        return build_report(
            cli,
            Some(&before),
            None,
            Some(&service),
            false,
            None,
            OUTCOME_FAILED_SERVICE_ACTIVE,
            reason,
            ACTION_SERVICE_ACTIVE,
            None,
        );
    }

    if before.wal.bytes < cli.min_wal_bytes {
        return build_report(
            cli,
            Some(&before),
            None,
            Some(&service),
            false,
            None,
            OUTCOME_SKIPPED_NOT_NEEDED,
            REASON_NOT_NEEDED,
            ACTION_NOT_NEEDED,
            None,
        );
    }

    if cli.dry_run {
        return build_report(
            cli,
            Some(&before),
            None,
            Some(&service),
            false,
            None,
            OUTCOME_SKIPPED_DRY_RUN,
            REASON_DRY_RUN,
            ACTION_DRY_RUN,
            None,
        );
    }

    match checkpoint(&db_path, cli.timeout_seconds) {
        Ok(result) if result.busy != 0 => build_report(
            cli,
            Some(&before),
            inspect_runtime_sqlite_files(&db_path).ok().as_ref(),
            Some(&service),
            true,
            Some(result),
            OUTCOME_FAILED_CHECKPOINT_BUSY,
            REASON_CHECKPOINT_BUSY,
            ACTION_BUSY,
            None,
        ),
        Ok(result) => {
            let after = match inspect_runtime_sqlite_files(&db_path) {
                Ok(after) => after,
                Err(error) => {
                    return failed_unproven_report(
                        cli,
                        Some(db_path),
                        Some(&before),
                        Some(&service),
                        Some(compact_error(error)),
                    );
                }
            };
            build_report(
                cli,
                Some(&before),
                Some(&after),
                Some(&service),
                true,
                Some(result),
                OUTCOME_COMPLETED,
                REASON_COMPLETED,
                ACTION_COMPLETED,
                None,
            )
        }
        Err(CheckpointFailure::Timeout(error)) => build_report(
            cli,
            Some(&before),
            inspect_runtime_sqlite_files(&db_path).ok().as_ref(),
            Some(&service),
            true,
            None,
            OUTCOME_FAILED_TIMEOUT,
            REASON_TIMEOUT,
            ACTION_TIMEOUT,
            Some(error),
        ),
        Err(CheckpointFailure::Unproven(error)) => failed_unproven_report(
            cli,
            Some(db_path),
            Some(&before),
            Some(&service),
            Some(error),
        ),
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

fn load_service_state_from_systemctl(service_name: &str) -> Result<ServiceState> {
    let output = Command::new("systemctl")
        .args([
            "show",
            service_name,
            "--property=ActiveState",
            "--property=SubState",
            "--no-pager",
        ])
        .output()
        .with_context(|| format!("failed to run systemctl show {service_name}"))?;
    if !output.status.success() {
        bail!(
            "systemctl show {service_name} failed with status {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    parse_systemctl_show_output(&String::from_utf8_lossy(&output.stdout))
}

fn parse_systemctl_show_output(output: &str) -> Result<ServiceState> {
    let mut active_state: Option<String> = None;
    let mut substate: Option<String> = None;
    for line in output.lines() {
        if let Some(value) = line.strip_prefix("ActiveState=") {
            active_state = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("SubState=") {
            substate = Some(value.trim().to_string());
        }
    }
    let active_state =
        active_state.ok_or_else(|| anyhow!("systemctl output missing ActiveState"))?;
    Ok(ServiceState {
        active_state: active_state.clone(),
        active: active_state == "active",
        substate,
    })
}

fn run_sqlite_wal_checkpoint_truncate(
    db_path: &Path,
    timeout_seconds: u64,
) -> Result<CheckpointResult, CheckpointFailure> {
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    let timed_out = Arc::new(AtomicBool::new(false));
    let timeout_flag = timed_out.clone();
    let conn = Connection::open(db_path).map_err(|error| {
        CheckpointFailure::Unproven(format!(
            "failed to open runtime SQLite DB for WAL maintenance {}: {error:#}",
            db_path.display()
        ))
    })?;
    let busy_timeout = Duration::from_secs(timeout_seconds).min(Duration::from_secs(5));
    conn.busy_timeout(busy_timeout).map_err(|error| {
        CheckpointFailure::Unproven(format!("failed to set SQLite busy_timeout: {error:#}"))
    })?;
    conn.progress_handler(
        1_000,
        Some(move || {
            let expired = Instant::now() >= deadline;
            if expired {
                timeout_flag.store(true, Ordering::SeqCst);
            }
            expired
        }),
    );
    let query_result = run_checkpoint_pragma(&conn);
    conn.progress_handler(0, None::<fn() -> bool>);

    match query_result {
        Ok(result) => Ok(result),
        Err(error) if timed_out.load(Ordering::SeqCst) => Err(CheckpointFailure::Timeout(format!(
            "SQLite-managed WAL checkpoint/truncate exceeded timeout_seconds={timeout_seconds}: {error:#}"
        ))),
        Err(error) => Err(CheckpointFailure::Unproven(format!(
            "failed running SQLite-managed WAL checkpoint/truncate: {error:#}"
        ))),
    }
}

fn run_checkpoint_pragma(conn: &Connection) -> Result<CheckpointResult> {
    let mut stmt = conn
        .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
        .context("failed preparing SQLite-managed WAL checkpoint/truncate")?;
    stmt.query_row([], |row| {
        Ok(CheckpointResult {
            busy: row.get(0)?,
            log_frames: row.get(1)?,
            checkpointed_frames: row.get(2)?,
        })
    })
    .context("failed executing SQLite-managed WAL checkpoint/truncate")
}

fn inspect_runtime_sqlite_files(db_path: &Path) -> Result<RuntimeSqliteFilesSnapshot> {
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
        modified_utc: metadata.modified().ok().map(DateTime::<Utc>::from),
    })
}

fn sqlite_sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = db_path.as_os_str().to_os_string();
    sidecar.push("-");
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

fn build_report(
    cli: &Cli,
    before: Option<&RuntimeSqliteFilesSnapshot>,
    after: Option<&RuntimeSqliteFilesSnapshot>,
    service: Option<&ServiceState>,
    checkpoint_attempted: bool,
    checkpoint_result: Option<CheckpointResult>,
    outcome: &str,
    reason: &str,
    action: &str,
    error: Option<String>,
) -> RuntimeSqliteWalMaintenanceReport {
    let final_wal_bytes = after
        .map(|snapshot| snapshot.wal.bytes)
        .or_else(|| before.map(|snapshot| snapshot.wal.bytes));
    RuntimeSqliteWalMaintenanceReport {
        production_green: false,
        dry_run: cli.dry_run,
        service_name: cli.service_name.clone(),
        service_active_state: service.map(|state| state.active_state.clone()),
        service_active: service.map(|state| state.active),
        service_substate: service.and_then(|state| state.substate.clone()),
        runtime_db_path: after
            .or(before)
            .map(|snapshot| snapshot.db_path.display().to_string()),
        before_db_bytes: before.map(|snapshot| snapshot.db.bytes),
        before_wal_bytes: before.map(|snapshot| snapshot.wal.bytes),
        before_shm_bytes: before.map(|snapshot| snapshot.shm.bytes),
        after_db_bytes: after.map(|snapshot| snapshot.db.bytes),
        after_wal_bytes: after.map(|snapshot| snapshot.wal.bytes),
        after_shm_bytes: after.map(|snapshot| snapshot.shm.bytes),
        min_wal_bytes: cli.min_wal_bytes,
        critical_wal_bytes: cli.critical_wal_bytes,
        checkpoint_attempted,
        checkpoint_busy: checkpoint_result.map(|result| result.busy),
        checkpoint_log_frames: checkpoint_result.map(|result| result.log_frames),
        checkpoint_checkpointed_frames: checkpoint_result.map(|result| result.checkpointed_frames),
        maintenance_outcome: outcome.to_string(),
        reason: reason.to_string(),
        final_wal_pressure_level: final_wal_bytes
            .map(|bytes| classify_wal_pressure(bytes, cli.min_wal_bytes, cli.critical_wal_bytes))
            .unwrap_or("unproven")
            .to_string(),
        service_safe_next_action: action.to_string(),
        error,
    }
}

fn failed_unproven_report(
    cli: &Cli,
    db_path: Option<PathBuf>,
    before: Option<&RuntimeSqliteFilesSnapshot>,
    service: Option<&ServiceState>,
    error: Option<String>,
) -> RuntimeSqliteWalMaintenanceReport {
    let fallback_before = db_path.as_ref().map(|path| RuntimeSqliteFilesSnapshot {
        db_path: path.to_path_buf(),
        db: FileMetadataSnapshot {
            exists: false,
            bytes: 0,
            modified_utc: None,
        },
        wal: FileMetadataSnapshot {
            exists: false,
            bytes: 0,
            modified_utc: None,
        },
        shm: FileMetadataSnapshot {
            exists: false,
            bytes: 0,
            modified_utc: None,
        },
    });
    build_report(
        cli,
        before.or(fallback_before.as_ref()),
        None,
        service,
        false,
        None,
        OUTCOME_FAILED_UNPROVEN,
        REASON_UNPROVEN,
        ACTION_UNPROVEN,
        error,
    )
}

fn classify_wal_pressure(
    wal_bytes: u64,
    min_wal_bytes: u64,
    critical_wal_bytes: u64,
) -> &'static str {
    if wal_bytes >= critical_wal_bytes {
        "critical"
    } else if wal_bytes >= min_wal_bytes {
        "large"
    } else {
        "none"
    }
}

fn resolve_db_path(config_path: &Path, sqlite_path: &str) -> PathBuf {
    let configured = Path::new(sqlite_path.trim());
    if configured.is_absolute() {
        return configured.to_path_buf();
    }
    match config_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(configured),
        _ => configured.to_path_buf(),
    }
}

fn compact_error(error: anyhow::Error) -> String {
    format!("{error:#}").replace('\n', " ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::sync::atomic::AtomicUsize;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_cli(db_path: PathBuf) -> Cli {
        Cli {
            config_path: PathBuf::from("unused.toml"),
            db_path_override: Some(db_path),
            service_name: "copybot-test.service".to_string(),
            json: true,
            min_wal_bytes: 1_000,
            critical_wal_bytes: 2_000,
            timeout_seconds: 30,
            dry_run: false,
            allow_service_active: false,
        }
    }

    fn service_state(active_state: &str, substate: &str) -> ServiceState {
        ServiceState {
            active_state: active_state.to_string(),
            active: active_state == "active",
            substate: Some(substate.to_string()),
        }
    }

    fn inactive_service(_: &str) -> Result<ServiceState> {
        Ok(service_state("inactive", "dead"))
    }

    fn active_service(_: &str) -> Result<ServiceState> {
        Ok(service_state("active", "running"))
    }

    fn successful_checkpoint(_: &Path, _: u64) -> Result<CheckpointResult, CheckpointFailure> {
        Ok(CheckpointResult {
            busy: 0,
            log_frames: 10,
            checkpointed_frames: 10,
        })
    }

    fn temp_test_dir(name: &str) -> Result<PathBuf> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let dir = env::temp_dir().join(format!(
            "copybot-runtime-wal-maintenance-{name}-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed creating temp dir {}", dir.display()))?;
        Ok(dir)
    }

    fn create_file_with_len(path: &Path, bytes: u64) -> Result<()> {
        let mut file =
            File::create(path).with_context(|| format!("failed creating {}", path.display()))?;
        if bytes > 0 {
            file.write_all(&[0])?;
            file.set_len(bytes)
                .with_context(|| format!("failed sizing {}", path.display()))?;
        }
        Ok(())
    }

    fn seed_sqlite_wal(db_path: &Path, rows: usize) -> Result<Connection> {
        let conn = Connection::open(db_path)
            .with_context(|| format!("failed opening WAL seed DB {}", db_path.display()))?;
        conn.busy_timeout(Duration::from_millis(250))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "wal_autocheckpoint", 0_i64)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS wal_seed(
                id INTEGER PRIMARY KEY,
                payload TEXT NOT NULL
            );",
        )?;
        let payload = "runtime-wal-maintenance-ballast".repeat(32);
        for idx in 0..rows.max(1) {
            conn.execute(
                "INSERT INTO wal_seed(id, payload) VALUES (?1, ?2)",
                rusqlite::params![idx as i64, payload],
            )?;
        }
        Ok(conn)
    }

    #[test]
    fn active_service_without_allow_fails_before_sqlite_checkpoint() -> Result<()> {
        let dir = temp_test_dir("active-service")?;
        let db_path = dir.join("runtime.db");
        create_file_with_len(&db_path, 128)?;
        create_file_with_len(&sqlite_sidecar_path(&db_path, "wal"), 2_500)?;
        let cli = test_cli(db_path);
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_checkpoint = attempts.clone();

        let report = run_with_hooks(&cli, active_service, move |_, _| {
            attempts_for_checkpoint.fetch_add(1, Ordering::SeqCst);
            successful_checkpoint(Path::new("unused"), 30)
        });

        assert_eq!(report.maintenance_outcome, OUTCOME_FAILED_SERVICE_ACTIVE);
        assert_eq!(report.reason, REASON_SERVICE_ACTIVE);
        assert_eq!(report.service_active_state.as_deref(), Some("active"));
        assert!(!report.checkpoint_attempted);
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn non_inactive_service_states_fail_before_sqlite_checkpoint() -> Result<()> {
        let cases = [
            ("active", "running", REASON_SERVICE_ACTIVE),
            ("activating", "start", REASON_SERVICE_NOT_INACTIVE),
            ("deactivating", "stop-sigterm", REASON_SERVICE_NOT_INACTIVE),
            ("reloading", "reload", REASON_SERVICE_NOT_INACTIVE),
            ("failed", "failed", REASON_SERVICE_NOT_INACTIVE),
        ];

        for (active_state, substate, expected_reason) in cases {
            let dir = temp_test_dir(active_state)?;
            let db_path = dir.join("runtime.db");
            create_file_with_len(&db_path, 128)?;
            create_file_with_len(&sqlite_sidecar_path(&db_path, "wal"), 2_500)?;
            let cli = test_cli(db_path);
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_for_checkpoint = attempts.clone();

            let report = run_with_hooks(
                &cli,
                |_| Ok(service_state(active_state, substate)),
                move |_, _| {
                    attempts_for_checkpoint.fetch_add(1, Ordering::SeqCst);
                    successful_checkpoint(Path::new("unused"), 30)
                },
            );

            assert_eq!(report.maintenance_outcome, OUTCOME_FAILED_SERVICE_ACTIVE);
            assert_eq!(report.reason, expected_reason);
            assert_eq!(report.service_active_state.as_deref(), Some(active_state));
            assert_eq!(report.service_substate.as_deref(), Some(substate));
            assert!(!report.checkpoint_attempted);
            assert_eq!(attempts.load(Ordering::SeqCst), 0);
            assert!(!report.production_green);
        }

        Ok(())
    }

    #[test]
    fn allow_service_active_allows_non_inactive_state_to_reach_checkpoint() -> Result<()> {
        let dir = temp_test_dir("allow-non-inactive")?;
        let db_path = dir.join("runtime.db");
        create_file_with_len(&db_path, 128)?;
        create_file_with_len(&sqlite_sidecar_path(&db_path, "wal"), 2_500)?;
        let mut cli = test_cli(db_path);
        cli.allow_service_active = true;
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_checkpoint = attempts.clone();

        let report = run_with_hooks(
            &cli,
            |_| Ok(service_state("activating", "start")),
            move |_, _| {
                attempts_for_checkpoint.fetch_add(1, Ordering::SeqCst);
                successful_checkpoint(Path::new("unused"), 30)
            },
        );

        assert_eq!(report.maintenance_outcome, OUTCOME_COMPLETED);
        assert_eq!(report.reason, REASON_COMPLETED);
        assert_eq!(report.service_active_state.as_deref(), Some("activating"));
        assert!(report.checkpoint_attempted);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn inactive_service_wal_below_min_skips_checkpoint() -> Result<()> {
        let dir = temp_test_dir("not-needed")?;
        let db_path = dir.join("runtime.db");
        create_file_with_len(&db_path, 128)?;
        create_file_with_len(&sqlite_sidecar_path(&db_path, "wal"), 999)?;
        let cli = test_cli(db_path);

        let report = run_with_hooks(&cli, inactive_service, successful_checkpoint);

        assert_eq!(report.maintenance_outcome, OUTCOME_SKIPPED_NOT_NEEDED);
        assert_eq!(report.reason, REASON_NOT_NEEDED);
        assert_eq!(report.service_active_state.as_deref(), Some("inactive"));
        assert!(!report.checkpoint_attempted);
        assert_eq!(report.final_wal_pressure_level, "none");
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn report_json_includes_service_active_state() -> Result<()> {
        let dir = temp_test_dir("json-service-state")?;
        let db_path = dir.join("runtime.db");
        create_file_with_len(&db_path, 128)?;
        create_file_with_len(&sqlite_sidecar_path(&db_path, "wal"), 999)?;
        let cli = test_cli(db_path);

        let report = run_with_hooks(&cli, inactive_service, successful_checkpoint);
        let json = serde_json::to_value(&report)?;

        assert_eq!(json["service_active_state"], "inactive");
        assert_eq!(json["service_substate"], "dead");
        assert_eq!(json["service_active"], false);
        assert_eq!(json["production_green"], false);
        Ok(())
    }

    #[test]
    fn dry_run_large_wal_skips_checkpoint_and_reports_would_run() -> Result<()> {
        let dir = temp_test_dir("dry-run")?;
        let db_path = dir.join("runtime.db");
        create_file_with_len(&db_path, 128)?;
        create_file_with_len(&sqlite_sidecar_path(&db_path, "wal"), 1_500)?;
        let mut cli = test_cli(db_path);
        cli.dry_run = true;

        let report = run_with_hooks(&cli, inactive_service, successful_checkpoint);

        assert_eq!(report.maintenance_outcome, OUTCOME_SKIPPED_DRY_RUN);
        assert_eq!(report.reason, REASON_DRY_RUN);
        assert_eq!(report.service_safe_next_action, ACTION_DRY_RUN);
        assert!(!report.checkpoint_attempted);
        assert_eq!(report.final_wal_pressure_level, "large");
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn inactive_service_large_wal_runs_checkpoint_and_reduces_wal_metadata() -> Result<()> {
        let dir = temp_test_dir("checkpoint-success")?;
        let db_path = dir.join("runtime.db");
        let _seed_conn = seed_sqlite_wal(&db_path, 64)?;
        let before_wal = inspect_runtime_sqlite_files(&db_path)?.wal.bytes;
        assert!(before_wal >= 1, "test setup must create a WAL file");
        let mut cli = test_cli(db_path.clone());
        cli.min_wal_bytes = 1;
        cli.critical_wal_bytes = before_wal.saturating_add(1).max(2);

        let report = run_with_hooks(&cli, inactive_service, run_sqlite_wal_checkpoint_truncate);

        assert_eq!(report.maintenance_outcome, OUTCOME_COMPLETED);
        assert_eq!(report.reason, REASON_COMPLETED);
        assert!(report.checkpoint_attempted);
        assert_eq!(report.checkpoint_busy, Some(0));
        assert!(report.after_wal_bytes.unwrap_or(u64::MAX) < before_wal);
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn busy_checkpoint_returns_failed_checkpoint_busy() -> Result<()> {
        let dir = temp_test_dir("busy")?;
        let db_path = dir.join("runtime.db");
        create_file_with_len(&db_path, 128)?;
        create_file_with_len(&sqlite_sidecar_path(&db_path, "wal"), 2_500)?;
        let cli = test_cli(db_path);

        let report = run_with_hooks(&cli, inactive_service, |_, _| {
            Ok(CheckpointResult {
                busy: 1,
                log_frames: 11,
                checkpointed_frames: 7,
            })
        });

        assert_eq!(report.maintenance_outcome, OUTCOME_FAILED_CHECKPOINT_BUSY);
        assert_eq!(report.reason, REASON_CHECKPOINT_BUSY);
        assert!(report.checkpoint_attempted);
        assert_eq!(report.checkpoint_busy, Some(1));
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn timeout_returns_failed_timeout() -> Result<()> {
        let dir = temp_test_dir("timeout")?;
        let db_path = dir.join("runtime.db");
        create_file_with_len(&db_path, 128)?;
        create_file_with_len(&sqlite_sidecar_path(&db_path, "wal"), 2_500)?;
        let cli = test_cli(db_path);

        let report = run_with_hooks(&cli, inactive_service, |_, _| {
            Err(CheckpointFailure::Timeout("timeout for test".to_string()))
        });

        assert_eq!(report.maintenance_outcome, OUTCOME_FAILED_TIMEOUT);
        assert_eq!(report.reason, REASON_TIMEOUT);
        assert!(report.checkpoint_attempted);
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn production_green_is_always_false_for_all_outcomes() -> Result<()> {
        let outcomes = [
            OUTCOME_SKIPPED_NOT_NEEDED,
            OUTCOME_SKIPPED_DRY_RUN,
            OUTCOME_FAILED_SERVICE_ACTIVE,
            OUTCOME_FAILED_CHECKPOINT_BUSY,
            OUTCOME_FAILED_TIMEOUT,
            OUTCOME_FAILED_UNPROVEN,
            OUTCOME_COMPLETED,
        ];
        let cli = test_cli(PathBuf::from("/tmp/runtime.db"));
        for outcome in outcomes {
            let report = build_report(
                &cli, None, None, None, false, None, outcome, "reason", "action", None,
            );
            assert!(!report.production_green, "outcome={outcome}");
        }
        Ok(())
    }

    #[test]
    fn source_omits_manual_sidecar_mutation_calls() {
        let source = include_str!("copybot_runtime_sqlite_wal_maintenance.rs");
        assert!(!source.contains(concat!("remove", "_file")));
        assert!(!source.contains(concat!("remove", "_dir")));
        assert!(!source.contains(concat!("fs::", "rename")));
        assert!(!source.contains(concat!(".", "rename", "(")));
    }

    #[test]
    fn parses_systemctl_show_output() -> Result<()> {
        let state = parse_systemctl_show_output("ActiveState=inactive\nSubState=dead\n")?;
        assert_eq!(state.active_state, "inactive");
        assert!(!state.active);
        assert!(state.service_inactive_for_maintenance());
        assert_eq!(state.substate.as_deref(), Some("dead"));
        let active = parse_systemctl_show_output("SubState=running\nActiveState=active\n")?;
        assert_eq!(active.active_state, "active");
        assert!(active.active);
        assert!(!active.service_inactive_for_maintenance());
        assert_eq!(active.substate.as_deref(), Some("running"));
        Ok(())
    }
}
