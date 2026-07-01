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
fn deferred_rebuild_reports_not_green_and_keeps_source_pristine() -> Result<()> {
    let db_path = setup_deferred_db("happy", true)?;
    let output_path = sibling_path(&db_path, "compact");
    let _ = std::fs::remove_file(&output_path);
    let output = run_deferred_rebuild(&db_path, &output_path)?;
    let parsed = parse_output(output.stdout)?;

    assert!(!output.status.success());
    assert_eq!(
        parsed["maintenance_outcome"],
        "completed_integrity_deferred"
    );
    assert_eq!(parsed["production_green"], false);
    assert_eq!(parsed["full_integrity_deferred"], true);
    assert_eq!(
        parsed["required_full_integrity_marker"],
        "full_integrity=ok"
    );
    assert_eq!(parsed["rebuild_integrity_check"], Value::Null);
    assert_eq!(parsed["rebuild_quick_check"], "ok");
    assert_eq!(parsed["rebuild_foreign_key_violations"], 0);
    assert_eq!(parsed["rebuild_row_count_mismatches"], 0);
    assert_eq!(parsed["rebuild_schema_mismatches"], 0);
    assert!(parsed["service_safe_next_action"]
        .as_str()
        .unwrap_or_default()
        .contains("full integrity NOT run"));
    assert!(parsed["service_safe_next_action"]
        .as_str()
        .unwrap_or_default()
        .contains("full_integrity=ok marker"));
    let timings = parsed["rebuild_phase_timings_ms"]
        .as_object()
        .context("missing phase timings")?;
    assert!(timings.contains_key("copy_tables"));
    assert!(timings.contains_key("create_indexes"));
    assert!(timings.contains_key("cheap_checks"));
    assert!(timings.contains_key("foreign_key_check"));
    assert!(!timings.contains_key("integrity_check"));

    assert_eq!(count_rows(&db_path, "observed_swaps")?, 2);
    assert_eq!(count_rows(&output_path, "observed_swaps")?, 1);
    assert_eq!(count_rows(&output_path, "followlist")?, 1);
    assert_eq!(count_rows(&output_path, "wallet_metrics")?, 1);
    assert_eq!(count_rows(&output_path, "shadow_closed_trades")?, 1);
    Ok(())
}

#[test]
fn deferred_rebuild_cheap_checks_fail_on_missing_critical_state() -> Result<()> {
    let db_path = setup_deferred_db("critical-empty", false)?;
    let output_path = sibling_path(&db_path, "compact");
    let _ = std::fs::remove_file(&output_path);
    let output = run_deferred_rebuild(&db_path, &output_path)?;
    let parsed = parse_output(output.stdout)?;

    assert!(!output.status.success());
    assert_eq!(parsed["maintenance_outcome"], "failed_unproven");
    let error = parsed["error"].as_str().unwrap_or_default();
    assert!(error.contains("compact rebuild cheap checks failed"));
    assert!(error.contains("critical_empty_tables"));
    Ok(())
}

fn run_deferred_rebuild(db_path: &Path, output_path: &Path) -> Result<std::process::Output> {
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
            "--defer-rebuild-integrity-check",
        ])
        .env("PATH", path)
        .output()
        .context("failed running deferred rebuild retention operator")
}

fn setup_deferred_db(name: &str, critical_rows: bool) -> Result<PathBuf> {
    let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
    let db_path = std::env::temp_dir().join(format!("copybot-retention-deferred-{name}-{id}.db"));
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
        );
        CREATE TABLE followlist (
            wallet_id TEXT PRIMARY KEY
        );
        CREATE TABLE wallet_metrics (
            wallet_id TEXT PRIMARY KEY,
            trades INTEGER NOT NULL
        );
        CREATE TABLE shadow_closed_trades (
            id INTEGER PRIMARY KEY,
            pnl_sol REAL NOT NULL,
            closed_ts TEXT NOT NULL
        );",
    )?;
    let old_ts = (Utc::now() - Duration::days(60)).to_rfc3339();
    let new_ts = Utc::now().to_rfc3339();
    insert_observed(&conn, "old", &old_ts)?;
    insert_observed(&conn, "new", &new_ts)?;
    conn.execute(
        "INSERT INTO execution_quote_canary_events(event_id, request_ts) VALUES (?1, ?2)",
        params!["new", &new_ts],
    )?;
    conn.execute(
        "INSERT INTO execution_quote_canary_provider_samples(provider, request_ts)
         VALUES (?1, ?2)",
        params!["provider", &new_ts],
    )?;
    conn.execute(
        "INSERT INTO execution_quote_canary_shadow_gate_events(signal_id, recorded_ts)
         VALUES (?1, ?2)",
        params!["signal", &new_ts],
    )?;
    if critical_rows {
        conn.execute("INSERT INTO followlist(wallet_id) VALUES ('wallet')", [])?;
        conn.execute(
            "INSERT INTO wallet_metrics(wallet_id, trades) VALUES ('wallet', 7)",
            [],
        )?;
        conn.execute(
            "INSERT INTO shadow_closed_trades(id, pnl_sol, closed_ts)
             VALUES (1, 0.1, ?1)",
            params![&new_ts],
        )?;
    }
    Ok(db_path)
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

fn fake_systemctl_dir(active_state: &str) -> Result<PathBuf> {
    let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("copybot-deferred-fake-systemctl-{id}"));
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

fn count_rows(db_path: &Path, table: &str) -> Result<i64> {
    let conn = Connection::open(db_path)?;
    Ok(
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })?,
    )
}

fn sibling_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".");
    value.push(suffix);
    PathBuf::from(value)
}
