use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_operators::execution_canary_readiness::build_report_from_db_path;
use copybot_storage_core::{ExecutionCanaryBuildPlanMetadata, SqliteStore};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use tempfile::TempDir;

pub struct Fixture {
    pub dir: TempDir,
    pub db: PathBuf,
    pub config: PathBuf,
}

impl Fixture {
    pub fn new(known: bool) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let db = dir.path().join("synthetic.db");
        let config = dir.path().join("synthetic.toml");
        let store = SqliteStore::open(&db)?;
        store.ensure_history_retention_tables()?;
        let now = Utc::now() - chrono::Duration::seconds(10);
        let order = store
            .reserve_execution_canary_order("readiness-config-fixture", "metis-canary", now)?
            .order;
        store.record_execution_canary_build_plan_metadata(&ExecutionCanaryBuildPlanMetadata {
            http_request_started_ts: None,
            quote_response_available_ts: None,
            order_id: order.order_id,
            signal_id: order.signal_id,
            client_order_id: order.client_order_id,
            recorded_ts: now,
            quote_source: Some("execution_quote_canary_event".into()),
            quote_event_id: Some(
                if known {
                    "quote:known"
                } else {
                    "quote:unknown"
                }
                .into(),
            ),
            quote_status: Some("ok".into()),
            quote_in_amount_raw: Some("10000000".into()),
            quote_out_amount_raw: Some("123456".into()),
            quote_response_json: None,
            quote_request_ts: None,
            quote_price_sol: Some(0.000081),
            price_impact_pct: Some(0.04),
            route_plan_json: None,
            priority_fee_source: None,
            priority_fee_status: known.then(|| "ok".into()),
            priority_fee_lamports: known.then_some(12_345),
            priority_fee_json: None,
            slippage_bps: Some(12.5),
            decision_status: known.then(|| "would_execute".into()),
            decision_reason: known.then(|| "inside_limits".into()),
        })?;
        drop(store);
        let conn = Connection::open(&db)?;
        conn.execute_batch(
            "PRAGMA journal_mode=DELETE;
             INSERT INTO schema_migrations(version,applied_at)
             VALUES('readiness-test-sentinel','2026-01-01T00:00:00Z');",
        )?;
        drop(conn);
        write_config(&config, &db)?;
        Ok(Self { dir, db, config })
    }

    pub fn cli(&self, label: &str, args: &[&str], expected_exit: i32) -> Result<Value> {
        let before = snapshot(self.dir.path())?;
        let binary = env!("CARGO_BIN_EXE_copybot_execution_canary_readiness");
        let output = Command::new(binary)
            .args(args)
            .current_dir(self.dir.path())
            .stdin(Stdio::null())
            .output()?;
        let unchanged = before == snapshot(self.dir.path())?;
        if let Some(path) = std::env::var_os("BATCH36_CLI_EVIDENCE") {
            let record = json!({"case": label, "binary": binary, "argv": args,
                "exit_code": output.status.code(), "stdout": String::from_utf8_lossy(&output.stdout),
                "stderr": String::from_utf8_lossy(&output.stderr), "fixture_bytes_modes_unchanged": unchanged});
            writeln!(
                OpenOptions::new().create(true).append(true).open(path)?,
                "{record}"
            )?;
        }
        assert!(
            unchanged,
            "{label}: CLI changed fixture files, schema, data, migrations or config"
        );
        assert_eq!(
            output.status.code(),
            Some(expected_exit),
            "{label}: {output:?}"
        );
        assert!(output.stderr.is_empty(), "{label}: unexpected stderr");
        Ok(serde_json::from_slice(&output.stdout)?)
    }
}

pub fn path(path: &Path) -> &str {
    path.to_str().expect("synthetic UTF-8 path")
}

pub fn write_config(config: &Path, db: &Path) -> Result<()> {
    fs::write(
        config,
        format!("[sqlite]\npath = {}\n", serde_json::to_string(path(db))?),
    )?;
    assert_eq!(
        copybot_config::load_from_path(config)?.sqlite.path,
        path(db)
    );
    Ok(())
}

// Snapshot the complete fixture, including sidecars and migration/config bytes.
fn snapshot(root: &Path) -> Result<BTreeMap<PathBuf, (u32, Vec<u8>)>> {
    fn visit(root: &Path, dir: &Path, files: &mut BTreeMap<PathBuf, (u32, Vec<u8>)>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let p = entry.path();
            let metadata = entry.metadata()?;
            let bytes = if metadata.is_dir() {
                Vec::new()
            } else {
                fs::read(&p)?
            };
            files.insert(
                p.strip_prefix(root)?.to_path_buf(),
                (metadata.permissions().mode(), bytes),
            );
            if metadata.is_dir() {
                visit(root, &p, files)?;
            }
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files)?;
    Ok(files)
}

pub fn assert_db_report(report: &Value, db: &Path, loaded: bool) -> Result<()> {
    let as_of =
        DateTime::parse_from_rfc3339(report["as_of"].as_str().unwrap())?.with_timezone(&Utc);
    let helper = build_report_from_db_path(db, as_of);
    assert!(
        !helper.config_loaded,
        "public DB helper has no config to load"
    );
    let mut actual = report.clone();
    actual["config_loaded"] = json!(false);
    assert_eq!(
        actual,
        serde_json::to_value(helper)?,
        "all other fields must match the DB helper at the same timestamp"
    );
    assert_eq!(
        report["config_loaded"], loaded,
        "config_loaded must reflect successful loading, independently of final success"
    );
    Ok(())
}

pub fn assert_success(report: &Value, known: bool) {
    assert_eq!(report["reason_class"], "execution_canary_readiness_loaded");
    assert_eq!(report["db_opened"], true);
    assert!(report["error"].is_null());
    assert_eq!(report["readiness_green"], known);
    assert_eq!(report["economic_green"], false);
    assert_eq!(report["production_green"], false);
    assert_eq!(
        report["readiness_basis"],
        "quote_simulation_only_not_economic"
    );
    assert_eq!(
        report["summary"]["readiness_status"],
        if known { "would_enter" } else { "unknown" }
    );
    assert_eq!(report["summary"]["total_orders"], 1);
    assert_eq!(
        report["summary"]["latest"]["quote_event_id"],
        if known {
            "quote:known"
        } else {
            "quote:unknown"
        }
    );
    let fee = if known { json!(12_345) } else { Value::Null };
    assert_eq!(report["summary"]["latest"]["priority_fee_lamports"], fee);
    assert_eq!(report["window"]["latest_priority_fee_lamports"], fee);
    assert_eq!(report["window"]["total_orders"], 1);
    assert_eq!(report["window"]["would_enter_orders"], u64::from(known));
    assert_eq!(report["window"]["unknown_orders"], u64::from(!known));
    assert_eq!(report["window"]["limit"], 50);
    assert_eq!(report["failed_expenses"]["coverage"], "schema_unavailable");
    for field in [
        "known_wallet_fee_lamports",
        "cohort_wallet_fee_lamports",
        "economic_pnl_lamports",
    ] {
        assert!(
            report["failed_expenses"][field].is_null(),
            "Unknown must remain null: {field}"
        );
    }
}

pub fn assert_failure(report: &Value, reason: &str, error: &str) {
    // Compare the complete legacy failure shape before checking config_loaded separately.
    assert_eq!(
        *report,
        json!({
            "config_loaded": report["config_loaded"], "db_opened": false,
            "as_of": report["as_of"], "reason_class": format!("execution_canary_readiness_{reason}"),
            "error": error, "readiness_green": false,
            "readiness_basis": "quote_simulation_only_not_economic", "economic_green": false,
            "failed_expenses": null, "production_green": false, "summary": null, "window": null
        })
    );
}
