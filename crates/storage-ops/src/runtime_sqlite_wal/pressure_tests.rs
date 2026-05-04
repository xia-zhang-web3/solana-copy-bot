use super::*;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
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

    let error =
        inspect_runtime_sqlite_files(&db_path).expect_err("missing DB metadata must fail closed");
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
    let source = include_str!("pressure.rs");
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
