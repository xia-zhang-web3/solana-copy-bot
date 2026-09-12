#![allow(dead_code)]
use crate::quote_portfolio_report_support::*;
use copybot_storage_core::{ExecutionQuoteCanaryEventInsert, SqliteStore};
use serde_json::{json, Value};
use std::{fs, os::unix::fs::PermissionsExt};

pub fn fixture(name: &str) -> Fixture {
    let f = Fixture::new(name);
    fs::set_permissions(&f.db, fs::Permissions::from_mode(0o644)).unwrap();
    let s = SqliteStore::open(&f.db).unwrap();
    for (id, n, side, ins, outs) in [
        ("a60", 1, "buy", 60, 100),
        ("b35", 2, "buy", 35, 100),
        ("b40", 2, "buy", 40, 100),
        ("ma", 1, "sell", 100, 60),
        ("mb35", 2, "sell", 100, 35),
        ("mb40", 2, "sell", 100, 40),
        ("unit", 1, "buy", 1, 1),
        ("ten", 1, "sell", 1, 10),
    ] {
        s.record_execution_quote_canary_event(&row(id, n, side, ins, outs))
            .unwrap();
    }
    drop(s);
    let c = rusqlite::Connection::open(&f.db).unwrap();
    c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")
        .unwrap();
    drop(c);
    fs::set_permissions(&f.db, fs::Permissions::from_mode(0o444)).unwrap();
    fs::set_permissions(
        f.dir.join("source.before.sqlite"),
        fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    fs::copy(&f.db, f.dir.join("source.before.sqlite")).unwrap();
    f
}
fn row(id: &str, n: u8, side: &str, input: u64, output: u64) -> ExecutionQuoteCanaryEventInsert {
    let (im, om, idec, odec) = if side == "buy" {
        (SOL.to_string(), mint(n), 9, 0)
    } else {
        (mint(n), SOL.to_string(), 0, 9)
    };
    ExecutionQuoteCanaryEventInsert {
        event_id: id.into(),
        wallet_id: "fixture-wallet".into(),
        signal_id: None,
        shadow_closed_trade_id: None,
        token: mint(n),
        side: side.into(),
        quote_status: "ok".into(),
        request_ts: TS.parse().unwrap(),
        http_request_started_ts: Some(TS.parse().unwrap()),
        quote_response_available_ts: Some(TS.parse().unwrap()),
        signal_ts: None,
        decision_delay_ms: None,
        quote_latency_ms: None,
        leader_notional_sol: None,
        quote_in_amount_raw: Some(input.to_string()),
        quote_out_amount_raw: Some(output.to_string()),
        quote_response_json: Some(
            json!({"inputMint":im,"outputMint":om,"inAmount":input.to_string(),
            "outAmount":output.to_string(),"inputDecimals":idec,"outputDecimals":odec})
            .to_string(),
        ),
        quote_price_sol: None,
        shadow_price_sol: None,
        slippage_bps: None,
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        decision_status: None,
        decision_reason: None,
        error: None,
    }
}
pub fn fee(id: &str, seq: u64, amount: Value) -> Value {
    event(
        id,
        "portfolio-fee",
        seq,
        json!({"kind":"failed_attempt_expense","amount":amount}),
    )
}
pub fn mark(id: &str, pos: &str, seq: u64, n: u8, gross: u64) -> Value {
    event(
        id,
        pos,
        seq,
        json!({"kind":"mark","quote":reference(id,pos,n,"sell",100,gross),
        "costs":costs(id,pos,"sell")}),
    )
}
pub fn matrix(charge: u64, size: u64, marks: bool, legacy: bool) -> Value {
    let mut expense = fee("fee", 2, amount(charge));
    if legacy {
        expense["action"]["kind"] = json!("unsupported_expense");
        expense["action"]["description"] = json!("failed transaction base fee");
    }
    let mut events = vec![
        buy("a60", "A", 1, 1, 60, 100),
        expense,
        buy(&format!("b{size}"), "B", 3, 2, size, 100),
    ];
    if marks {
        events.push(mark("ma", "A", 4, 1, 60));
        if legacy || charge + size <= 40 {
            events.push(mark(&format!("mb{size}"), "B", 5, 2, size));
        }
    }
    scenario(100, 2, events)
}
pub fn pair(f: &Fixture, name: &str, input: &Value) -> Value {
    let path = f.dir.join(format!("{name}.input.json"));
    fs::write(&path, serde_json::to_vec_pretty(input).unwrap()).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).unwrap();
    let db_meta = fs::metadata(&f.db).unwrap();
    let input_meta = fs::metadata(&path).unwrap();
    let v = f.pair_path(name, Some(&path));
    assert_eq!(
        db_meta.permissions().mode(),
        fs::metadata(&f.db).unwrap().permissions().mode()
    );
    assert_eq!(
        input_meta.permissions().mode(),
        fs::metadata(&path).unwrap().permissions().mode()
    );
    assert_eq!(
        db_meta.modified().unwrap(),
        fs::metadata(&f.db).unwrap().modified().unwrap()
    );
    assert_eq!(
        input_meta.modified().unwrap(),
        fs::metadata(&path).unwrap().modified().unwrap()
    );
    v
}
pub fn state(v: &Value, index: usize) -> &Value {
    &v["events"][index]["outcome"]["disposition"]["state"]
}
pub fn code(v: &Value, index: usize) -> &Value {
    &v["events"][index]["outcome"]["disposition"]["refusal"]["code"]
}
pub fn unknown(v: &Value) {
    assert_eq!(v["production_green"], false);
    assert!(v["dataset_coverage"]["unknown"].is_string());
    if v["status"] == "replayed" {
        assert!(v["dataset_net_change_lamports"]["unknown"].is_string());
    } else {
        assert_eq!(v["status"], "unavailable");
        assert!(v.get("book").is_none());
    }
}
