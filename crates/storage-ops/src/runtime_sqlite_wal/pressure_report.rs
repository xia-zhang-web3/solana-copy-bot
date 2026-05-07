use super::types::{
    Cli, RuntimeSqliteWalPressureReport, WalPressureLevel, ACTION_CRITICAL, ACTION_LARGE,
    ACTION_NONE, ACTION_UNPROVEN, REASON_CRITICAL, REASON_LARGE, REASON_NONE,
    REASON_UNPROVEN_METADATA,
};
use crate::runtime_sqlite_wal::common::{
    inspect_runtime_sqlite_files, resolve_db_path, RuntimeSqliteFilesSnapshot,
};
use anyhow::{bail, Context, Result};
use copybot_config::load_from_path;
use std::path::PathBuf;

pub(super) fn run(cli: &Cli) -> Result<RuntimeSqliteWalPressureReport> {
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

pub(super) fn build_report_from_snapshot(
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

pub(super) fn classify_wal_pressure(
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

pub(super) fn unproven_report(
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

pub(super) fn report_should_render_json(report: &RuntimeSqliteWalPressureReport) -> bool {
    report.metadata_error.as_deref() != Some("runtime_sqlite_wal_pressure_json_required")
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
