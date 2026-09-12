#![allow(dead_code)]
#[path = "../quote_portfolio_report_support/mod.rs"]
mod prior;
use copybot_core_types::{CopySignalRow, ExactSwapAmounts, Lamports, SwapEvent};
use copybot_storage_core::{ExecutionQuoteCanaryEventInsert, SqliteStore};
pub use prior::{amount, costs, event, mint, reference, scenario, BINS, SOL, TS};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub fn origin() -> Value {
    prior::origin()
}

pub fn signature(n: u8) -> String {
    bs58::encode([n; 64]).into_string()
}
pub fn signal(n: u8, side: &str) -> String {
    format!("shadow:{}:fixture-wallet:{side}:{}", signature(n), mint(1))
}
pub fn source(n: u8) -> Value {
    json!({"kind":"observed_signature","signature":signature(n)})
}
pub fn quote_ref(id: &str, side: &str, input: u64, output: u64) -> Value {
    let mut q = reference(id, "A", 1, side, input, output);
    q["decimals"] = json!(3);
    q["signal_id"] = json!(signal(71, "buy"));
    q["decimals_source"] = source(71);
    q
}
pub fn buy() -> Value {
    event(
        "buy",
        "A",
        1,
        json!({"kind":"buy","mint":mint(1),"decimals":3,
        "input_lamports":"10000000","quote":quote_ref("buy","buy",10_000_000,10_000),
        "costs":costs("buy","A","buy"),"rent_deposit":amount(0)}),
    )
}
pub fn lifecycle(bound: bool) -> Value {
    let mut b = buy();
    b["action"]["costs"]["base"] = amount(5_000);
    b["action"]["costs"]["priority"] = amount(7_003);
    b["action"]["costs"]["setup"] = amount(10_000);
    b["action"]["rent_deposit"] = amount(2_039_280);
    let mut s = event(
        "sell",
        "A",
        2,
        json!({"kind":"sell","raw":"4000",
        "quote":quote_ref("sell","sell",4_000,3_125_000),"costs":costs("sell","A","sell")}),
    );
    s["action"]["costs"]["base"] = amount(5_000);
    s["action"]["costs"]["priority"] = amount(3_007);
    s["action"]["costs"]["exit"] = amount(1_000);
    let m = event(
        "mark",
        "A",
        3,
        json!({"kind":"mark",
        "quote":quote_ref("mark","sell",6_000,4_500_000),"costs":costs("mark","A","sell")}),
    );
    let mut i = scenario(1_000_000_000, 1, vec![b, s, m]);
    if !bound {
        for e in i["events"].as_array_mut().unwrap() {
            e["action"]["quote"]
                .as_object_mut()
                .unwrap()
                .remove("decimals_source");
        }
    }
    i
}
// Exact generic app_tests/82.rs literal, with only the fixture mint substituted.
// SELL/Mark change only direction/mints/raw/threshold and meta side. No root decimals.
pub fn payload(side: &str, input: u64, output: u64) -> String {
    let app = include_str!("../../../app/src/app_tests/82.rs");
    let raw = app
        .lines()
        .find_map(|l| l.trim().strip_prefix("r#\"{\"inputMint\""))
        .unwrap()
        .strip_suffix("\"#")
        .unwrap();
    let raw = format!("{{\"inputMint\"{raw}").replace("TokenMint", &mint(1));
    if side == "buy" && input == 10_000_000 && output == 10_000 {
        return raw;
    }
    let mut v: Value = serde_json::from_str(&raw).unwrap();
    v["inputMint"] = json!(if side == "buy" {
        SOL.to_string()
    } else {
        mint(1)
    });
    v["outputMint"] = json!(if side == "buy" {
        mint(1)
    } else {
        SOL.to_string()
    });
    v["inAmount"] = json!(input.to_string());
    v["outAmount"] = json!(output.to_string());
    v["otherAmountThreshold"] = json!(output.to_string());
    v["meta"] = if side == "buy" {
        json!({"outDecimals":3})
    } else {
        json!({"inDecimals":3})
    };
    v.to_string()
}
pub struct Fixture {
    pub dir: PathBuf,
    pub db: PathBuf,
    _temp: Option<tempfile::TempDir>,
}
impl Fixture {
    pub fn new(name: &str, tweak: impl FnOnce(&SqliteStore, &rusqlite::Connection)) -> Self {
        let (dir, temp) = match std::env::var_os("BATCH77_ACTUAL") {
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
        for n in [71, 72] {
            insert_observed(&store, n, "buy");
            store
                .insert_copy_signal(&CopySignalRow {
                    signal_id: signal(n, "buy"),
                    wallet_id: "fixture-wallet".into(),
                    side: "buy".into(),
                    token: mint(1),
                    notional_sol: 8.0,
                    notional_lamports: Some(Lamports::new(8_000_000_000)),
                    notional_origin: "leader_exact_lamports".into(),
                    ts: TS.parse().unwrap(),
                    status: "shadow_recorded".into(),
                })
                .unwrap();
        }
        for (id, side, input, output, n) in [
            ("buy", "buy", 10_000_000, 10_000, 71),
            ("sell", "sell", 4_000, 3_125_000, 71),
            ("mark", "sell", 6_000, 4_500_000, 71),
            ("alternate", "buy", 10_000_000, 10_000, 72),
        ] {
            store
                .record_execution_quote_canary_event(&row(id, side, input, output, n))
                .unwrap();
        }
        let c = rusqlite::Connection::open(&db).unwrap();
        tweak(&store, &c);
        drop(store);
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
    pub fn pair(&self, name: &str, input: &Value) -> Value {
        let path = self.dir.join(format!("{name}.input.json"));
        fs::write(&path, serde_json::to_vec_pretty(input).unwrap()).unwrap();
        fs::copy(&path, self.dir.join(format!("{name}.input.before"))).unwrap();
        self.pair_path(name, Some(&path))
    }
    pub fn pair_path(&self, name: &str, path: Option<&Path>) -> Value {
        let before = fs::read(&self.db).unwrap();
        let input = path.map(|p| fs::read(p).unwrap());
        let mut reports = Vec::new();
        for bin in BINS {
            let (v, code) = prior::run(&prior::binary(bin), &self.db, path);
            fs::write(
                self.dir.join(format!("{name}.{bin}.json")),
                serde_json::to_vec_pretty(&v).unwrap(),
            )
            .unwrap();
            fs::write(
                self.dir.join(format!("{name}.{bin}.exit")),
                code.to_string(),
            )
            .unwrap();
            assert_eq!(code, 0, "{name}: {v}");
            reports.push(v["portfolio_replay"].clone());
        }
        assert_eq!(reports[0], reports[1]);
        assert_eq!(before, fs::read(&self.db).unwrap());
        assert_eq!(input, path.map(|p| fs::read(p).unwrap()));
        prior::integrity(&self.db);
        reports.remove(0)
    }
}
pub fn insert_observed(store: &SqliteStore, n: u8, side: &str) {
    let buy = side == "buy";
    store
        .insert_observed_swap(&SwapEvent {
            signature: signature(n),
            wallet: "fixture-wallet".into(),
            dex: "fixture".into(),
            token_in: if buy { SOL.to_string() } else { mint(1) },
            token_out: if buy { mint(1) } else { SOL.to_string() },
            amount_in: if buy { 8.0 } else { 999_999.999 },
            amount_out: if buy { 999_999.999 } else { 8.0 },
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: if buy { "8000000000" } else { "999999999" }.into(),
                amount_out_raw: if buy { "999999999" } else { "8000000000" }.into(),
                amount_in_decimals: if buy { 9 } else { 3 },
                amount_out_decimals: if buy { 3 } else { 9 },
            }),
            slot: 123,
            ts_utc: TS.parse().unwrap(),
        })
        .unwrap();
}
fn row(id: &str, side: &str, input: u64, output: u64, n: u8) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: id.into(),
        wallet_id: "fixture-wallet".into(),
        signal_id: Some(signal(n, "buy")),
        shadow_closed_trade_id: None,
        token: mint(1),
        side: side.into(),
        quote_status: "ok".into(),
        request_ts: TS.parse().unwrap(),
        // Synthetic fixture start, not observed HTTP provenance.
        http_request_started_ts: Some(TS.parse().unwrap()),
        quote_response_available_ts: Some(TS.parse().unwrap()),
        signal_ts: None,
        decision_delay_ms: None,
        quote_latency_ms: None,
        leader_notional_sol: Some(8.0),
        quote_in_amount_raw: Some(input.to_string()),
        quote_out_amount_raw: Some(output.to_string()),
        quote_response_json: Some(payload(side, input, output)),
        quote_price_sol: None,
        shadow_price_sol: None,
        slippage_bps: None,
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_status: Some("ok".into()),
        priority_fee_lamports: Some(999_999),
        priority_fee_json: None,
        decision_status: None,
        decision_reason: None,
        error: None,
    }
}
pub fn assert_unknown(v: &Value, reason: &str) {
    assert_eq!(v["status"], "replayed", "{v}");
    let e = &v["events"][0];
    assert_eq!(e["source_binding"]["state"], "unavailable", "{v}");
    assert!(
        e["source_binding"]["reason"]
            .as_str()
            .unwrap()
            .contains(reason),
        "{reason}: {v}"
    );
    assert_eq!(e["outcome"]["disposition"]["state"], "refused");
    assert_eq!(e["outcome"]["before"], e["outcome"]["after"]);
    assert_eq!(v["book"]["cash_lamports"], "1000000000");
    assert!(v["valuation"]["full_equity_lamports"]["unknown"].is_string());
}
