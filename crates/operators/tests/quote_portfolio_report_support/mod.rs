#![allow(dead_code)]
mod input;
use copybot_storage_core::{ExecutionQuoteCanaryEventInsert, SqliteStore};
pub use input::*;
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

pub const BINS: [&str; 2] = [
    "copybot_execution_canary_quote_pnl",
    "copybot_execution_tiny_economics",
];
pub struct Fixture {
    pub dir: PathBuf,
    pub db: PathBuf,
    _temp: Option<tempfile::TempDir>,
}
impl Fixture {
    pub fn new(name: &str) -> Self {
        let (dir, temp) = match std::env::var_os("BATCH76_ACTUAL") {
            Some(root) => {
                let d = PathBuf::from(root).join(name);
                fs::create_dir(&d).unwrap();
                (d, None)
            }
            None => {
                let t = tempfile::tempdir().unwrap();
                (t.path().to_path_buf(), Some(t))
            }
        };
        let db = dir.join("source.sqlite");
        let mut store = SqliteStore::open(&db).unwrap();
        store
            .run_migrations(Path::new(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../migrations"
            )))
            .unwrap();
        for (id, n, side, ins, outs) in [
            ("buy-a", 1, "buy", 600_000_000, 100),
            ("buy-b", 2, "buy", 600_000_000, 100),
            ("sell-a40", 1, "sell", 40, 312_500_000),
            ("mark-a60", 1, "sell", 60, 450_000_000),
            ("mark-a40", 1, "sell", 40, 312_500_000),
            ("small", 1, "buy", 100, 100),
            ("small-alt", 1, "buy", 100, 100),
            ("next", 2, "buy", 1, 1),
            ("zero-sell", 1, "sell", 100, 0),
            ("refund-sell", 1, "sell", 100, 110),
            ("large", 1, "buy", 9_007_199_254_740_993, 100),
        ] {
            store
                .record_execution_quote_canary_event(&row(id, n, side, ins, outs))
                .unwrap();
        }
        for case in [
            "raw",
            "mint",
            "side",
            "status",
            "wallet",
            "time",
            "response_raw",
            "response_mint",
            "decimals",
            "missing_decimals",
            "missing_response",
            "ambiguous",
            "missing_raw",
        ] {
            let id = format!("bad-{case}");
            let mut r = row(&id, 1, "buy", 100, 100);
            match case {
                "raw" => r.quote_out_amount_raw = Some("101".into()),
                "mint" => r.token = mint(2),
                "side" => r.side = "sell".into(),
                "status" => r.quote_status = "error".into(),
                "wallet" => r.wallet_id = "other".into(),
                "time" => r.request_ts += chrono::Duration::seconds(1),
                "missing_response" => r.quote_response_json = None,
                "missing_raw" => r.quote_out_amount_raw = None,
                "ambiguous" => {
                    r.quote_response_json = Some(r.quote_response_json.unwrap().replacen(
                        '{',
                        "{\"outputDecimals\":1,",
                        1,
                    ))
                }
                other => {
                    let mut v: Value =
                        serde_json::from_str(r.quote_response_json.as_ref().unwrap()).unwrap();
                    match other {
                        "response_raw" => v["outAmount"] = json!("101"),
                        "response_mint" => v["outputMint"] = json!(mint(2)),
                        "decimals" => v["outputDecimals"] = json!(1),
                        "missing_decimals" => {
                            v.as_object_mut().unwrap().remove("outputDecimals");
                        }
                        _ => unreachable!(),
                    }
                    r.quote_response_json = Some(v.to_string());
                }
            }
            store.record_execution_quote_canary_event(&r).unwrap();
        }
        drop(store);
        let c = rusqlite::Connection::open(&db).unwrap();
        c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")
            .unwrap();
        drop(c);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&db, fs::Permissions::from_mode(0o444)).unwrap();
        }
        fs::copy(&db, dir.join("source.before.sqlite")).unwrap();
        Self {
            dir,
            db,
            _temp: temp,
        }
    }
    pub fn pair(&self, name: &str, input: Option<&Value>) -> Value {
        let path = input.map(|v| {
            let p = self.dir.join(format!("{name}.input.json"));
            fs::write(&p, serde_json::to_vec_pretty(v).unwrap()).unwrap();
            p
        });
        self.pair_path(name, path.as_deref())
    }
    pub fn pair_path(&self, name: &str, path: Option<&Path>) -> Value {
        let db_before = fs::read(&self.db).unwrap();
        let input_before = path.map(|p| fs::read(p).unwrap());
        if let Some(bytes) = &input_before {
            fs::write(self.dir.join(format!("{name}.input.before")), bytes).unwrap();
        }
        let mut values = Vec::new();
        for bin in BINS {
            let (value, code) = run(&binary(bin), &self.db, path);
            fs::write(
                self.dir.join(format!("{name}.{bin}.json")),
                serde_json::to_vec_pretty(&value).unwrap(),
            )
            .unwrap();
            fs::write(
                self.dir.join(format!("{name}.{bin}.exit")),
                code.to_string(),
            )
            .unwrap();
            assert_eq!(code, 0, "legacy report failed: {value}");
            values.push(value["portfolio_replay"].clone());
        }
        assert_eq!(values[0], values[1], "shared portfolio JSON differs");
        assert_eq!(db_before, fs::read(&self.db).unwrap(), "DB bytes changed");
        assert_eq!(
            input_before,
            path.map(|p| fs::read(p).unwrap()),
            "input bytes changed"
        );
        integrity(&self.db);
        values.remove(0)
    }
}
pub fn binary(bin: &str) -> PathBuf {
    PathBuf::from(std::env::var_os("BATCH76_BIN_DIR").expect("set narrow dev binary directory"))
        .join(bin)
}
pub fn run(bin: &Path, db: &Path, input: Option<&Path>) -> (Value, i32) {
    let mut c = Command::new(bin);
    c.args(["--json", "--db-path"])
        .arg(db)
        .args(["--since", TS]);
    if bin.file_name().unwrap() == BINS[1] {
        c.arg("--no-live-wallet");
    }
    if let Some(p) = input {
        c.arg("--portfolio-input").arg(p);
    }
    let out = c.output().unwrap();
    assert!(out.stderr.is_empty(), "{:?}", out.stderr);
    (
        serde_json::from_slice(&out.stdout).unwrap(),
        out.status.code().unwrap(),
    )
}
pub fn integrity(db: &Path) {
    let c = rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap();
    assert_eq!(
        c.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}
fn row(id: &str, n: u8, side: &str, input: u64, output: u64) -> ExecutionQuoteCanaryEventInsert {
    let (im, om, idec, odec) = if side == "buy" {
        (SOL.to_string(), mint(n), 9, 0)
    } else {
        (mint(n), SOL.to_string(), 0, 9)
    };
    ExecutionQuoteCanaryEventInsert { event_id:id.into(),wallet_id:"fixture-wallet".into(),
        signal_id:None,shadow_closed_trade_id:None,token:mint(n),side:side.into(),quote_status:"ok".into(),
        request_ts:chrono::DateTime::parse_from_rfc3339(TS).unwrap().with_timezone(&chrono::Utc),
        http_request_started_ts:Some(TS.parse().unwrap()),quote_response_available_ts:Some(TS.parse().unwrap()),signal_ts:None,decision_delay_ms:None,quote_latency_ms:None,
        leader_notional_sol:None,quote_in_amount_raw:Some(input.to_string()),quote_out_amount_raw:Some(output.to_string()),
        quote_response_json:Some(json!({"inputMint":im,"outputMint":om,"inAmount":input.to_string(),"outAmount":output.to_string(),
            "inputDecimals":idec,"outputDecimals":odec}).to_string()),quote_price_sol:None,shadow_price_sol:None,
        slippage_bps:None,price_impact_pct:None,route_plan_json:None,priority_fee_status:Some("ok".into()),
        priority_fee_lamports:Some(999_999),priority_fee_json:None,decision_status:None,decision_reason:None,error:None }
}
pub fn observed(v: &mut Value) {
    match v {
        Value::Object(o) => {
            if o.contains_key("synthetic") {
                o.remove("synthetic");
                o.insert("observed".into(), json!("caller assertion only"));
            }
            for value in o.values_mut() {
                observed(value);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(observed),
        _ => {}
    }
}
