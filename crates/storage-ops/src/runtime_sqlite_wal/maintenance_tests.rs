use super::*;
use anyhow::{Context, Result};
use rusqlite::Connection;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    let dir = std::env::temp_dir().join(format!(
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
    let source = include_str!("maintenance.rs");
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
