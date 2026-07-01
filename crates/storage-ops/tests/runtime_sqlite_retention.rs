use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use rusqlite::{params, Connection};
use serde_json::Value;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn dry_run_does_not_delete_runtime_rows() -> Result<()> {
    let db_path = setup_db("dry-run")?;
    let output = run_operator(&db_path, false, "inactive")?;

    assert_eq!(output["maintenance_outcome"], "dry_run");
    assert_eq!(count_rows(&db_path, "observed_swaps")?, 2);
    assert_eq!(count_rows(&db_path, "execution_quote_canary_events")?, 2);
    Ok(())
}

#[test]
fn active_service_blocks_commit_by_default() -> Result<()> {
    let db_path = setup_db("active-service")?;
    let output = run_operator_allow_failure(&db_path, true, "active")?;

    assert_eq!(output["maintenance_outcome"], "failed_service_active");
    assert_eq!(count_rows(&db_path, "observed_swaps")?, 2);
    Ok(())
}

#[test]
fn commit_deletes_only_rows_older_than_cutoffs() -> Result<()> {
    let db_path = setup_db("commit")?;
    let output = run_operator(&db_path, true, "inactive")?;

    assert_eq!(output["maintenance_outcome"], "completed");
    assert_eq!(output["production_green"], true);
    assert_eq!(output["observed_deleted_rows"], 1);
    assert_eq!(output["canary_event_deleted_rows"], 1);
    assert_eq!(output["canary_provider_sample_deleted_rows"], 1);
    assert_eq!(output["canary_shadow_gate_deleted_rows"], 1);
    assert_eq!(count_rows(&db_path, "observed_swaps")?, 1);
    assert_eq!(count_rows(&db_path, "observed_sol_leg_swaps")?, 1);
    assert_eq!(count_rows(&db_path, "execution_quote_canary_events")?, 1);
    Ok(())
}

#[test]
fn rebuild_into_creates_compact_retained_copy_without_mutating_source() -> Result<()> {
    let db_path = setup_db("rebuild")?;
    let output_path = sibling_path(&db_path, "compact");
    let _ = std::fs::remove_file(&output_path);
    let output = run_rebuild_operator(&db_path, &output_path)?;

    assert_eq!(output["maintenance_outcome"], "completed");
    assert_eq!(output["rebuild_attempted"], true);
    assert_eq!(output["rebuild_integrity_check"], "ok");
    assert_eq!(output["rebuild_foreign_key_violations"], 0);
    assert_eq!(count_rows(&db_path, "observed_swaps")?, 2);
    assert_eq!(count_rows(&output_path, "observed_swaps")?, 1);
    assert_eq!(count_rows(&output_path, "observed_sol_leg_swaps")?, 1);
    assert_eq!(
        count_rows(&output_path, "execution_quote_canary_events")?,
        1
    );
    assert!(index_exists(
        &output_path,
        "idx_execution_quote_canary_events_request_ts_only"
    )?);
    assert_eq!(auto_vacuum_mode(&output_path)?, 2);

    let conn = Connection::open(&output_path)?;
    insert_observed(&conn, "trigger-check", "2026-06-16T00:01:00+00:00")?;
    assert_eq!(count_rows(&output_path, "observed_swaps")?, 2);
    assert_eq!(count_rows(&output_path, "observed_sol_leg_swaps")?, 2);
    Ok(())
}

#[test]
fn rebuild_into_rejects_source_mutation_flags() -> Result<()> {
    let db_path = setup_db("rebuild-guard")?;
    let output_path = sibling_path(&db_path, "compact");
    let _ = std::fs::remove_file(&output_path);
    let fake_bin = fake_systemctl_dir("inactive")?;
    let binary = env!("CARGO_BIN_EXE_copybot_runtime_sqlite_retention_maintenance");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new(binary)
        .args([
            "--config",
            "dummy.toml",
            "--db-path",
            &db_path.display().to_string(),
            "--json",
            "--commit",
            "--max-observed-rows",
            "1",
            "--rebuild-into",
            &output_path.display().to_string(),
        ])
        .env("PATH", path)
        .output()
        .context("failed running rebuild guard operator")?;
    let parsed = parse_output(output.stdout)?;

    assert!(!output.status.success());
    assert_eq!(parsed["maintenance_outcome"], "failed_unproven");
    assert!(parsed["error"]
        .as_str()
        .unwrap_or_default()
        .contains("cannot be combined"));
    assert!(!output_path.exists());
    assert_eq!(count_rows(&db_path, "observed_swaps")?, 2);
    Ok(())
}

#[test]
fn rebuild_into_fails_loud_on_compact_foreign_key_violations() -> Result<()> {
    let db_path = setup_fk_retention_db("rebuild-fk")?;
    let output_path = sibling_path(&db_path, "compact");
    let _ = std::fs::remove_file(&output_path);
    let output = run_rebuild_operator_allow_failure(&db_path, &output_path)?;

    assert!(!output.status.success());
    let parsed = parse_output(output.stdout)?;
    assert_eq!(parsed["maintenance_outcome"], "failed_unproven");
    assert!(parsed["error"]
        .as_str()
        .unwrap_or_default()
        .contains("compact rebuild verification failed"));
    Ok(())
}

fn run_operator(db_path: &Path, commit: bool, active_state: &str) -> Result<Value> {
    let output = run_operator_raw(db_path, commit, active_state)?;
    if !output.status.success() {
        anyhow::bail!(
            "operator failed: status={:?} stderr={} stdout={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
    }
    parse_output(output.stdout)
}

fn run_operator_allow_failure(db_path: &Path, commit: bool, active_state: &str) -> Result<Value> {
    let output = run_operator_raw(db_path, commit, active_state)?;
    parse_output(output.stdout)
}

fn run_rebuild_operator(db_path: &Path, output_path: &Path) -> Result<Value> {
    let output = run_rebuild_operator_allow_failure(db_path, output_path)?;
    if !output.status.success() {
        anyhow::bail!(
            "rebuild operator failed: status={:?} stderr={} stdout={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
    }
    parse_output(output.stdout)
}

fn run_rebuild_operator_allow_failure(
    db_path: &Path,
    output_path: &Path,
) -> Result<std::process::Output> {
    let fake_bin = fake_systemctl_dir("inactive")?;
    let binary = env!("CARGO_BIN_EXE_copybot_runtime_sqlite_retention_maintenance");
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    Command::new(binary)
        .args([
            "--config",
            "dummy.toml",
            "--db-path",
            &db_path.display().to_string(),
            "--json",
            "--observed-retention-days",
            "3",
            "--canary-retention-days",
            "30",
            "--commit",
            "--rebuild-into",
            &output_path.display().to_string(),
        ])
        .env("PATH", path)
        .output()
        .context("failed running rebuild retention operator")
}

fn run_operator_raw(
    db_path: &Path,
    commit: bool,
    active_state: &str,
) -> Result<std::process::Output> {
    let fake_bin = fake_systemctl_dir(active_state)?;
    let binary = env!("CARGO_BIN_EXE_copybot_runtime_sqlite_retention_maintenance");
    let mut args = vec![
        "--config".to_string(),
        "dummy.toml".to_string(),
        "--db-path".to_string(),
        db_path.display().to_string(),
        "--json".to_string(),
        "--observed-retention-days".to_string(),
        "3".to_string(),
        "--canary-retention-days".to_string(),
        "30".to_string(),
        "--max-observed-rows".to_string(),
        "10".to_string(),
        "--max-canary-rows".to_string(),
        "10".to_string(),
    ];
    if commit {
        args.push("--commit".to_string());
    }
    let path = format!(
        "{}:{}",
        fake_bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    Command::new(binary)
        .args(args)
        .env("PATH", path)
        .output()
        .context("failed running retention maintenance operator")
}

fn fake_systemctl_dir(active_state: &str) -> Result<PathBuf> {
    let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("copybot-fake-systemctl-{id}"));
    std::fs::create_dir_all(&dir)?;
    let script = dir.join("systemctl");
    let substate = if active_state == "active" {
        "running"
    } else {
        "dead"
    };
    std::fs::write(
        &script,
        format!(
            "#!/usr/bin/env sh\nprintf 'ActiveState={active_state}\\nSubState={substate}\\n'\n"
        ),
    )?;
    let mut perms = std::fs::metadata(&script)?.permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms)?;
    Ok(dir)
}

fn parse_output(stdout: Vec<u8>) -> Result<Value> {
    serde_json::from_slice(&stdout)
        .with_context(|| format!("invalid JSON output: {}", String::from_utf8_lossy(&stdout)))
}

fn setup_db(name: &str) -> Result<PathBuf> {
    let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
    let db_path =
        std::env::temp_dir().join(format!("copybot-retention-maintenance-{name}-{id}.db"));
    let _ = std::fs::remove_file(&db_path);
    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "CREATE TABLE observed_swaps (
            signature TEXT PRIMARY KEY,
            wallet_id TEXT NOT NULL,
            token_in TEXT NOT NULL,
            token_out TEXT NOT NULL,
            amount_in REAL NOT NULL,
            amount_out REAL NOT NULL,
            slot INTEGER NOT NULL,
            ts TEXT NOT NULL
        );
        CREATE INDEX idx_observed_swaps_ts_slot_signature
            ON observed_swaps(ts, slot, signature);
        CREATE TABLE observed_sol_leg_swaps (
            signature TEXT PRIMARY KEY,
            wallet_id TEXT NOT NULL,
            is_buy INTEGER NOT NULL,
            token_mint TEXT NOT NULL,
            token_qty REAL NOT NULL,
            sol_notional REAL NOT NULL,
            slot INTEGER NOT NULL,
            ts TEXT NOT NULL
        );
        CREATE TRIGGER observed_swaps_sol_leg_projection_delete
        AFTER DELETE ON observed_swaps
        BEGIN
            DELETE FROM observed_sol_leg_swaps WHERE signature = OLD.signature;
        END;
        CREATE TABLE execution_quote_canary_events (
            event_id TEXT PRIMARY KEY,
            request_ts TEXT NOT NULL
        );
        CREATE TABLE execution_quote_canary_provider_samples (
            provider TEXT NOT NULL,
            request_ts TEXT NOT NULL
        );
        CREATE TABLE execution_quote_canary_shadow_gate_events (
            signal_id TEXT PRIMARY KEY,
            recorded_ts TEXT NOT NULL
        );",
    )?;
    let old_ts = days_ago(60);
    let new_ts = Utc::now().to_rfc3339();
    insert_observed(&conn, "old", &old_ts)?;
    insert_observed(&conn, "new", &new_ts)?;
    conn.execute(
        "INSERT INTO execution_quote_canary_events(event_id, request_ts) VALUES (?1, ?2), (?3, ?4)",
        params!["old", &old_ts, "new", &new_ts],
    )?;
    conn.execute(
        "INSERT INTO execution_quote_canary_provider_samples(provider, request_ts) VALUES (?1, ?2), (?3, ?4)",
        params![
            "old",
            &old_ts,
            "new",
            &new_ts
        ],
    )?;
    conn.execute(
        "INSERT INTO execution_quote_canary_shadow_gate_events(signal_id, recorded_ts) VALUES (?1, ?2), (?3, ?4)",
        params![
            "old",
            &old_ts,
            "new",
            &new_ts
        ],
    )?;
    Ok(db_path)
}

fn setup_fk_retention_db(name: &str) -> Result<PathBuf> {
    let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
    let db_path =
        std::env::temp_dir().join(format!("copybot-retention-maintenance-{name}-{id}.db"));
    let _ = std::fs::remove_file(&db_path);
    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "CREATE TABLE observed_swaps (
            signature TEXT PRIMARY KEY,
            wallet_id TEXT NOT NULL,
            token_in TEXT NOT NULL,
            token_out TEXT NOT NULL,
            amount_in REAL NOT NULL,
            amount_out REAL NOT NULL,
            slot INTEGER NOT NULL,
            ts TEXT NOT NULL
        );
        CREATE INDEX idx_observed_swaps_ts_slot_signature
            ON observed_swaps(ts, slot, signature);
        CREATE TABLE observed_notes (
            id INTEGER PRIMARY KEY,
            signature TEXT NOT NULL REFERENCES observed_swaps(signature)
        );",
    )?;
    conn.execute(
        "INSERT INTO observed_swaps(signature, wallet_id, token_in, token_out, amount_in, amount_out, slot, ts)
         VALUES ('old', 'wallet', 'SOL', 'TOKEN', 1.0, 2.0, 1, '2026-06-01T00:00:00+00:00')",
        [],
    )?;
    conn.execute(
        "INSERT INTO observed_notes(id, signature) VALUES (1, 'old')",
        [],
    )?;
    Ok(db_path)
}

fn sibling_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".");
    value.push(suffix);
    PathBuf::from(value)
}

fn days_ago(days: i64) -> String {
    (Utc::now() - Duration::days(days)).to_rfc3339()
}

fn insert_observed(conn: &Connection, signature: &str, ts: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO observed_swaps(signature, wallet_id, token_in, token_out, amount_in, amount_out, slot, ts)
         VALUES (?1, 'wallet', 'SOL', 'TOKEN', 1.0, 2.0, 1, ?2)",
        params![signature, ts],
    )?;
    conn.execute(
        "INSERT INTO observed_sol_leg_swaps(signature, wallet_id, is_buy, token_mint, token_qty, sol_notional, slot, ts)
         VALUES (?1, 'wallet', 1, 'TOKEN', 2.0, 1.0, 1, ?2)",
        params![signature, ts],
    )?;
    Ok(())
}

fn count_rows(db_path: &Path, table: &str) -> Result<i64> {
    let conn = Connection::open(db_path)?;
    Ok(
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })?,
    )
}

fn index_exists(db_path: &Path, index: &str) -> Result<bool> {
    let conn = Connection::open(db_path)?;
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name=?1)",
        [index],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

fn auto_vacuum_mode(db_path: &Path) -> Result<i64> {
    let conn = Connection::open(db_path)?;
    Ok(conn.query_row("PRAGMA auto_vacuum", [], |row| row.get(0))?)
}
