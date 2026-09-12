#![allow(dead_code)]
use super::http::{key, Account, Server};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    SqliteStore, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET, EXECUTION_CANARY_POSITION_STATE_OPEN,
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::fs::{self, File};
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub struct Pair {
    pub primary: Value,
    pub manual: Value,
    pub economics: Value,
    pub captures: Vec<Value>,
}
#[derive(Default)]
pub struct Options {
    pub fault: &'static str,
    pub db_mode: &'static str,
    pub limit: Option<u32>,
    pub manual_args: Option<Vec<String>>,
}
// Position tuples: synthetic mint, exact remaining raw (decimals 0), cost lamports.
pub fn run_case(
    name: &str,
    accounts: Vec<Account>,
    positions: &[(u8, u64, u64)],
    no_quote_base: bool,
) -> Result<Pair> {
    run_options(name, accounts, positions, no_quote_base, Options::default())
}
pub fn run_options(
    name: &str,
    accounts: Vec<Account>,
    positions: &[(u8, u64, u64)],
    no_quote_base: bool,
    options: Options,
) -> Result<Pair> {
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("wallet.db");
    let mut store = SqliteStore::open(&db)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let timestamp: DateTime<Utc> = "2026-09-05T10:00:00+00:00".parse()?;
    store.execution_canary_quote_pnl_summary(timestamp, timestamp, 200)?;
    store.execution_tiny_proof_report(timestamp, timestamp, 200)?;
    drop(store);
    let conn = Connection::open(&db)?;
    if options.db_mode == "duplicate_positions" {
        conn.execute_batch("DROP INDEX idx_positions_one_open_token_bucket")?;
    }
    for (index, (mint, raw, cost)) in positions.iter().enumerate() {
        conn.execute("INSERT INTO positions(position_id,token,qty,qty_raw,qty_decimals,cost_sol,cost_lamports,opened_ts,state,accounting_bucket) VALUES (?1,?2,?3,?4,0,?5,?6,?7,?8,?9)",params![format!("audit-position-{mint}-{index}"),key(*mint),*raw as f64,raw.to_string(),*cost as f64/1e9,*cost,"2026-09-05T10:00:00+00:00",EXECUTION_CANARY_POSITION_STATE_OPEN,EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET])?;
    }
    match options.db_mode {
        "missing_exact" => {
            conn.execute_batch("UPDATE positions SET qty_raw=NULL, qty_decimals=NULL")?;
        }
        "invalid_exact" => {
            conn.execute_batch("UPDATE positions SET qty_raw='invalid'")?;
        }
        "decimals_mismatch" => {
            conn.execute_batch("UPDATE positions SET qty_decimals=1")?;
        }
        "history" => {
            let ts = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false);
            for (mint, error) in [(1, "terminal_sell_no_route"), (2, "simulation_failed")] {
                conn.execute("INSERT INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,ts,status) VALUES (?1,'synthetic','sell',?2,1,?3,'failed')", params![format!("signal-{mint}"),key(mint),ts])?;
                conn.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,err_code,client_order_id) VALUES (?1,?2,'synthetic',?3,'execution_canary_failed',?4,?1)", params![format!("exec-canary:history-{mint}"),format!("signal-{mint}"),ts,error])?;
            }
        }
        _ => {}
    }
    let mut stmt=conn.prepare("SELECT position_id,token,qty_raw,cost_lamports,state,accounting_bucket FROM positions ORDER BY position_id")?;
    let rows=stmt.query_map([],|r|Ok(json!({"position_id":r.get::<_,String>(0)?,"token":r.get::<_,String>(1)?,"qty_raw":r.get::<_,Option<String>>(2)?,"cost_lamports":r.get::<_,i64>(3)?,"state":r.get::<_,String>(4)?,"accounting_bucket":r.get::<_,String>(5)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);
    drop(conn);
    let before = fs::read(&db)?;
    let mut reports = Vec::new();
    let mut all_captures = Vec::new();
    let mut binaries = vec![
        (
            "primary",
            env!("CARGO_BIN_EXE_copybot_execution_canary_quote_pnl"),
        ),
        (
            "economics",
            env!("CARGO_BIN_EXE_copybot_execution_tiny_economics"),
        ),
    ];
    if options.manual_args.is_some() {
        binaries.push((
            "manual",
            env!("CARGO_BIN_EXE_copybot_execution_tiny_writeoff"),
        ));
    }
    for (which, binary) in binaries {
        let server = Server::with_fault(accounts.clone(), options.fault)?;
        let config = dir.path().join("synthetic.toml");
        let quote_url = if no_quote_base { "" } else { &server.url };
        let config_text=format!("[sqlite]\npath = {:?}\n[execution]\nenabled = false\ncanary_tiny_submit_enabled = false\ncanary_wallet_pubkey = {:?}\npriority_fee_canary_rpc_url = {:?}\nquote_canary_base_url = {:?}\nquote_canary_timeout_ms = 1000\n",db.to_string_lossy(),key(99),format!("{}/rpc",server.url),quote_url);
        fs::write(&config, &config_text)?;
        let mut argv = vec![
            "--config".to_string(),
            config.to_string_lossy().into_owned(),
            "--json".into(),
            "--since".into(),
            "2026-09-05T00:00:00Z".into(),
        ];
        if which == "manual" {
            argv.truncate(3);
            argv.extend(options.manual_args.clone().unwrap());
        } else if let Some(limit) = options.limit {
            argv.extend(["--limit".into(), limit.to_string()]);
        }
        let mut command = Command::new(binary);
        command
            .args(&argv)
            .env_clear()
            .env("TMPDIR", dir.path())
            .current_dir(dir.path())
            .stdin(Stdio::null());
        if which == "primary" {
            command.arg("--wallet-reconciliation");
        }
        let stdout = dir.path().join("stdout.json");
        let stderr = dir.path().join("stderr.txt");
        command
            .stdout(Stdio::from(File::create(&stdout)?))
            .stderr(Stdio::from(File::create(&stderr)?));
        let start = Instant::now();
        let mut child = command.spawn()?;
        let mut killed = false;
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if start.elapsed() > Duration::from_secs(15) {
                child.kill()?;
                killed = true;
                break child.wait()?;
            }
            thread::sleep(Duration::from_millis(10));
        };
        let server_result = server.finish();
        let raw = fs::read_to_string(&stdout)?;
        let err = fs::read_to_string(&stderr)?;
        let capture = json!({"case":name,"cli":which,"binary":binary,"argv":argv,"primary_extra_flag":if which=="primary" {Some("--wallet-reconciliation")} else {None},"config":config_text,"exit_code":status.code(),"killed_at_deadline":killed,"elapsed_ms":start.elapsed().as_millis(),"stdout_raw":raw,"stderr":err,"rpc_accounts":accounts.iter().map(Account::row).collect::<Vec<_>>(),"db_positions":rows,"fixture_db_mode":options.db_mode,"fixture_rpc_fault":options.fault,"db_main_bytes_unchanged":before==fs::read(&db)?,"http":server_result.as_ref().ok(),"server_error":server_result.as_ref().err().map(ToString::to_string),"listener_joined":true});
        save_capture(&format!("{name}-{which}.json"), &capture)?;
        ensure!(status.success() && !killed, "CLI failure: {capture}");
        let http = server_result?;
        let nonzero = accounts.iter().filter(|a| a.raw > 0).count();
        ensure!(
            http.len()
                == 3 + if no_quote_base {
                    0
                } else if !options.fault.is_empty() && options.fault != "partial_rpc" {
                    nonzero.saturating_sub(1)
                } else {
                    nonzero
                },
            "unexpected HTTP call count"
        );
        ensure!(
            http.iter()
                .filter(|r| r["request"]["method"] == "POST")
                .count()
                == 3
        );
        ensure!(
            http.iter()
                .filter(|r| r["request"]["body"]["method"] == "getTokenAccountsByOwner")
                .count()
                == 2
        );
        ensure!(before == fs::read(&db)?, "read-only CLI changed DB bytes");
        let report: Value = serde_json::from_str(&raw)?;
        ensure!(report["error"].is_null(), "{report}");
        if which == "primary" {
            ensure!(report["config_loaded"] == true);
        }
        if which == "manual" {
            ensure!(report["commit"] == false && report["write_offs"] == json!([]));
        }
        reports.push(report);
        all_captures.push(capture);
    }
    ensure!(
        reports[0]["wallet_reconciliation"]["wallet_account_mark"]
            == reports[1]["wallet_account_mark"]
    );
    Ok(Pair {
        primary: reports.remove(0),
        economics: reports.remove(0),
        manual: if reports.is_empty() {
            Value::Null
        } else {
            reports.remove(0)
        },
        captures: all_captures,
    })
}
pub fn save_capture(name: &str, value: &Value) -> Result<()> {
    let Some(path) = std::env::var_os("BATCH72_CAPTURE_DIR") else {
        return Ok(());
    };
    fs::create_dir_all(&path)?;
    fs::write(
        Path::new(&path).join(name),
        serde_json::to_vec_pretty(value)?,
    )?;
    Ok(())
}
pub fn wallet(pair: &Pair) -> &Value {
    &pair.primary["wallet_reconciliation"]
}
pub fn row(pair: &Pair, id: u8) -> &Value {
    wallet(pair)["balances"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["token_account"] == key(id))
        .unwrap()
}
pub fn close(value: &Value, expected: f64) {
    assert!(
        (value.as_f64().expect("known numeric") - expected).abs() < 1e-10,
        "{value} != {expected}"
    );
}
pub fn same_other_reports(a: &Pair, b: &Pair) {
    let mut left = a.primary["summary"].clone();
    let mut right = b.primary["summary"].clone();
    left.as_object_mut().unwrap().remove("as_of");
    right.as_object_mut().unwrap().remove("as_of");
    assert_eq!(left, right);
    assert_eq!(a.economics["canary"], b.economics["canary"]);
    assert_eq!(a.economics["shadow"], b.economics["shadow"]);
    assert!(a.economics["tiny"]["realized_pnl_sol"].is_null());
    assert!(b.economics["tiny"]["realized_pnl_sol"].is_null());
}
