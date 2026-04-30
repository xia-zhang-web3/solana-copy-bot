use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

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

fn main() {
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(Some(cli)) => match run(&cli) {
            Ok(report) => report,
            Err(error) => unproven_report(None, &cli, Some(compact_error(error))),
        },
        Ok(None) => {
            println!("{USAGE}");
            return;
        }
        Err(error) => unproven_report(
            None,
            &Cli {
                config_path: PathBuf::new(),
                db_path_override: None,
                json: true,
                large_wal_threshold_bytes: DEFAULT_LARGE_WAL_THRESHOLD_BYTES,
                critical_wal_threshold_bytes: DEFAULT_CRITICAL_WAL_THRESHOLD_BYTES,
            },
            Some(compact_error(error)),
        ),
    };

    if report_should_render_json(&report) {
        println!(
            "{}",
            serde_json::to_string(&report).expect("WAL pressure report must serialize")
        );
    } else {
        println!(
            "wal_pressure_level={} wal_pressure_reason={} production_green=false",
            report.wal_pressure_level.as_str(),
            report.wal_pressure_reason
        );
    }
    std::process::exit(report.exit_code());
}

fn report_should_render_json(report: &RuntimeSqliteWalPressureReport) -> bool {
    report.metadata_error.as_deref() != Some("runtime_sqlite_wal_pressure_json_required")
}

fn run(cli: &Cli) -> Result<RuntimeSqliteWalPressureReport> {
    if !cli.json {
        return Ok(unproven_report(
            None,
            cli,
            Some("runtime_sqlite_wal_pressure_json_required".to_string()),
        ));
    }
    validate_thresholds(
        cli.large_wal_threshold_bytes,
        cli.critical_wal_threshold_bytes,
    )?;
    let loaded_config = load_from_path(&cli.config_path)
        .with_context(|| format!("failed loading config {}", cli.config_path.display()))?;
    let db_path = match &cli.db_path_override {
        Some(path) => path.to_path_buf(),
        None => resolve_db_path(&cli.config_path, &loaded_config.sqlite.path),
    };
    let snapshot = inspect_runtime_sqlite_files(&db_path).with_context(|| {
        format!(
            "failed proving runtime SQLite metadata for {}",
            db_path.display()
        )
    })?;
    Ok(build_report_from_snapshot(&snapshot, cli))
}

fn parse_args_from<I>(args: I) -> Result<Option<Cli>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut db_path_override: Option<PathBuf> = None;
    let mut json = false;
    let mut large_wal_threshold_bytes = DEFAULT_LARGE_WAL_THRESHOLD_BYTES;
    let mut critical_wal_threshold_bytes = DEFAULT_CRITICAL_WAL_THRESHOLD_BYTES;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--db-path" => {
                db_path_override = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?));
            }
            "--json" => json = true,
            "--large-wal-threshold-bytes" => {
                large_wal_threshold_bytes =
                    parse_u64_arg("--large-wal-threshold-bytes", args.next())?;
            }
            "--critical-wal-threshold-bytes" => {
                critical_wal_threshold_bytes =
                    parse_u64_arg("--critical-wal-threshold-bytes", args.next())?;
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Cli {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path_override,
        json,
        large_wal_threshold_bytes,
        critical_wal_threshold_bytes,
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

fn validate_thresholds(large: u64, critical: u64) -> Result<()> {
    if large == 0 {
        bail!("--large-wal-threshold-bytes must be greater than zero");
    }
    if critical < large {
        bail!("--critical-wal-threshold-bytes must be greater than or equal to --large-wal-threshold-bytes");
    }
    Ok(())
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
        modified_utc: metadata.modified().ok().and_then(system_time_to_utc),
    })
}

fn system_time_to_utc(time: SystemTime) -> Option<DateTime<Utc>> {
    Some(DateTime::<Utc>::from(time))
}

fn sqlite_sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = db_path.as_os_str().to_os_string();
    sidecar.push("-");
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

fn build_report_from_snapshot(
    snapshot: &RuntimeSqliteFilesSnapshot,
    cli: &Cli,
) -> RuntimeSqliteWalPressureReport {
    let (level, reason, action, manual_operator_action_required) = classify_wal_pressure(
        snapshot.wal.bytes,
        cli.large_wal_threshold_bytes,
        cli.critical_wal_threshold_bytes,
    );
    RuntimeSqliteWalPressureReport {
        production_green: false,
        runtime_db_path: Some(snapshot.db_path.display().to_string()),
        db_bytes: Some(snapshot.db.bytes),
        wal_bytes: Some(snapshot.wal.bytes),
        shm_bytes: Some(snapshot.shm.bytes),
        wal_exists: Some(snapshot.wal.exists),
        shm_exists: Some(snapshot.shm.exists),
        db_mtime_utc: snapshot.db.modified_utc,
        wal_mtime_utc: snapshot.wal.modified_utc,
        shm_mtime_utc: snapshot.shm.modified_utc,
        large_wal_threshold_bytes: cli.large_wal_threshold_bytes,
        critical_wal_threshold_bytes: cli.critical_wal_threshold_bytes,
        wal_pressure_level: level,
        wal_pressure_reason: reason.to_string(),
        service_safe_next_action: action.to_string(),
        manual_operator_action_required,
        metadata_error: None,
    }
}

fn classify_wal_pressure(
    wal_bytes: u64,
    large_threshold: u64,
    critical_threshold: u64,
) -> (WalPressureLevel, &'static str, &'static str, bool) {
    if wal_bytes >= critical_threshold {
        (
            WalPressureLevel::Critical,
            REASON_CRITICAL,
            ACTION_CRITICAL,
            true,
        )
    } else if wal_bytes >= large_threshold {
        (WalPressureLevel::Large, REASON_LARGE, ACTION_LARGE, false)
    } else {
        (WalPressureLevel::None, REASON_NONE, ACTION_NONE, false)
    }
}

fn unproven_report(
    runtime_db_path: Option<PathBuf>,
    cli: &Cli,
    metadata_error: Option<String>,
) -> RuntimeSqliteWalPressureReport {
    RuntimeSqliteWalPressureReport {
        production_green: false,
        runtime_db_path: runtime_db_path.map(|path| path.display().to_string()),
        db_bytes: None,
        wal_bytes: None,
        shm_bytes: None,
        wal_exists: None,
        shm_exists: None,
        db_mtime_utc: None,
        wal_mtime_utc: None,
        shm_mtime_utc: None,
        large_wal_threshold_bytes: cli.large_wal_threshold_bytes,
        critical_wal_threshold_bytes: cli.critical_wal_threshold_bytes,
        wal_pressure_level: WalPressureLevel::Unproven,
        wal_pressure_reason: REASON_UNPROVEN_METADATA.to_string(),
        service_safe_next_action: ACTION_UNPROVEN.to_string(),
        manual_operator_action_required: false,
        metadata_error,
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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_cli(large: u64, critical: u64) -> Cli {
        Cli {
            config_path: PathBuf::from("unused.toml"),
            db_path_override: None,
            json: true,
            large_wal_threshold_bytes: large,
            critical_wal_threshold_bytes: critical,
        }
    }

    fn temp_test_dir(name: &str) -> Result<PathBuf> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let dir = env::temp_dir().join(format!(
            "copybot-runtime-wal-pressure-{name}-{}-{unique}",
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

    fn inspect_report(db_path: &Path, large: u64, critical: u64) -> RuntimeSqliteWalPressureReport {
        let snapshot = inspect_runtime_sqlite_files(db_path).expect("metadata should be readable");
        build_report_from_snapshot(&snapshot, &test_cli(large, critical))
    }

    #[test]
    fn missing_wal_reports_no_pressure() -> Result<()> {
        let dir = temp_test_dir("missing-wal")?;
        let db_path = dir.join("runtime.db");
        create_file_with_len(&db_path, 128)?;

        let report = inspect_report(&db_path, 1_000, 2_000);
        assert_eq!(report.wal_pressure_level, WalPressureLevel::None);
        assert_eq!(report.wal_pressure_reason, REASON_NONE);
        assert_eq!(report.wal_exists, Some(false));
        assert_eq!(report.wal_bytes, Some(0));
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn small_wal_reports_no_pressure() -> Result<()> {
        let dir = temp_test_dir("small-wal")?;
        let db_path = dir.join("runtime.db");
        create_file_with_len(&db_path, 128)?;
        create_file_with_len(&sqlite_sidecar_path(&db_path, "wal"), 999)?;

        let report = inspect_report(&db_path, 1_000, 2_000);
        assert_eq!(report.wal_pressure_level, WalPressureLevel::None);
        assert_eq!(report.wal_pressure_reason, REASON_NONE);
        assert_eq!(report.wal_exists, Some(true));
        assert_eq!(report.wal_bytes, Some(999));
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn large_wal_reports_large_pressure() -> Result<()> {
        let dir = temp_test_dir("large-wal")?;
        let db_path = dir.join("runtime.db");
        create_file_with_len(&db_path, 128)?;
        create_file_with_len(&sqlite_sidecar_path(&db_path, "wal"), 1_500)?;

        let report = inspect_report(&db_path, 1_000, 2_000);
        assert_eq!(report.wal_pressure_level, WalPressureLevel::Large);
        assert_eq!(report.wal_pressure_reason, REASON_LARGE);
        assert_eq!(report.service_safe_next_action, ACTION_LARGE);
        assert!(!report.manual_operator_action_required);
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn critical_wal_reports_critical_pressure() -> Result<()> {
        let dir = temp_test_dir("critical-wal")?;
        let db_path = dir.join("runtime.db");
        create_file_with_len(&db_path, 128)?;
        create_file_with_len(&sqlite_sidecar_path(&db_path, "wal"), 2_000)?;

        let report = inspect_report(&db_path, 1_000, 2_000);
        assert_eq!(report.wal_pressure_level, WalPressureLevel::Critical);
        assert_eq!(report.wal_pressure_reason, REASON_CRITICAL);
        assert_eq!(report.service_safe_next_action, ACTION_CRITICAL);
        assert!(report.manual_operator_action_required);
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn missing_db_path_metadata_error_reports_unproven_fail_closed() -> Result<()> {
        let dir = temp_test_dir("missing-db")?;
        let db_path = dir.join("missing.db");
        let cli = test_cli(1_000, 2_000);

        let error = inspect_runtime_sqlite_files(&db_path)
            .expect_err("missing DB metadata must fail closed");
        let report = unproven_report(Some(db_path), &cli, Some(compact_error(error)));
        assert_eq!(report.wal_pressure_level, WalPressureLevel::Unproven);
        assert_eq!(report.wal_pressure_reason, REASON_UNPROVEN_METADATA);
        assert_eq!(report.db_bytes, None);
        assert!(report.metadata_error.is_some());
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn production_green_is_always_false_for_all_pressure_levels() -> Result<()> {
        for wal_bytes in [0_u64, 999, 1_000, 2_000] {
            let dir = temp_test_dir(&format!("production-green-{wal_bytes}"))?;
            let db_path = dir.join("runtime.db");
            create_file_with_len(&db_path, 128)?;
            if wal_bytes > 0 {
                create_file_with_len(&sqlite_sidecar_path(&db_path, "wal"), wal_bytes)?;
            }
            let report = inspect_report(&db_path, 1_000, 2_000);
            assert!(!report.production_green);
        }
        Ok(())
    }

    #[test]
    fn source_omits_mutating_sqlite_checkpoint_and_file_deletion_calls() {
        let source = include_str!("copybot_runtime_sqlite_wal_pressure_report.rs");
        assert!(!source.contains(concat!("wal_", "checkpoint")));
        assert!(!source.contains(concat!("remove_", "file")));
        assert!(!source.contains(concat!("PR", "AGMA")));
    }

    #[test]
    fn parse_args_accepts_threshold_overrides_and_db_override() -> Result<()> {
        let cli = parse_args_from([
            "--config".to_string(),
            "configs/dev.toml".to_string(),
            "--db-path".to_string(),
            "/tmp/runtime.db".to_string(),
            "--json".to_string(),
            "--large-wal-threshold-bytes".to_string(),
            "10".to_string(),
            "--critical-wal-threshold-bytes".to_string(),
            "20".to_string(),
        ])?
        .expect("parse should return config");
        assert_eq!(cli.config_path, PathBuf::from("configs/dev.toml"));
        assert_eq!(cli.db_path_override, Some(PathBuf::from("/tmp/runtime.db")));
        assert!(cli.json);
        assert_eq!(cli.large_wal_threshold_bytes, 10);
        assert_eq!(cli.critical_wal_threshold_bytes, 20);
        Ok(())
    }
}
