fn report_is_json_requested(report: &RuntimeSqliteWalMaintenanceReport) -> bool {
    report.error.as_deref() != Some("runtime_sqlite_wal_maintenance_json_required")
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
