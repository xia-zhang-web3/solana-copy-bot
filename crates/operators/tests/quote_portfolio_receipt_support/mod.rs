#![allow(dead_code)]
#[path = "../../../storage-core/tests/common/receipt_operand_fixture.rs"]
mod fixture;
mod quotes;
use crate::quote_portfolio_report_support as cli;
use copybot_operators::quote_portfolio_report;
pub use fixture::{Db, SIGNATURE, WALLET};
use serde_json::{json, Value};
use std::{fs, path::Path, process::Command};

pub fn reference(id: &str) -> Value {
    json!({"order_id":id,"tx_signature":SIGNATURE,"wallet":WALLET,"payer":WALLET,
        "operation_at":fixture::TS,"recorded_at":fixture::TS,
        "source_provenance":{"synthetic":"canonical-writer fixture, no chain receipt"}})
}
pub fn expense(id: &str, fee: u64) -> Value {
    cli::event(
        "fee",
        "unrelated-virtual-position",
        2,
        json!({"kind":"failed_attempt_expense","amount":cli::amount(fee),"receipt_ref":reference(id)}),
    )
}
pub fn input(id: &str, fee: u64) -> Value {
    cli::scenario(
        100,
        2,
        vec![expense(id, fee), cli::buy("b40", "B", 3, 2, 40, 100)],
    )
}
pub fn combined(db: &Db) {
    for (id, n, side, input, output) in [
        ("a60", 1, "buy", 60, 100),
        ("b40", 2, "buy", 40, 100),
        ("ma", 1, "sell", 100, 60),
    ] {
        db.store
            .record_execution_quote_canary_event(&quotes::row(id, n, side, input, output))
            .unwrap();
    }
}
pub fn evaluate(db: &Db, name: &str, input: &Value) -> Value {
    let path = db.path.with_file_name(format!("{name}.input.json"));
    fs::write(&path, serde_json::to_vec_pretty(input).unwrap()).unwrap();
    quote_portfolio_report::build(Some(&path), Some(&db.path))
}
pub fn pair(db: &Db, name: &str, input: &Value) -> Value {
    let frozen = db.freeze();
    pair_path(&frozen, name, input)
}
pub fn pair_path(db: &Path, name: &str, input: &Value) -> Value {
    let dir = db.parent().unwrap();
    let path = dir.join(format!("{name}.input.json"));
    fs::write(&path, serde_json::to_vec_pretty(input).unwrap()).unwrap();
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&path, permissions).unwrap();
    let before = [
        (db, fs::read(db).unwrap(), fs::metadata(db).unwrap()),
        (
            path.as_path(),
            fs::read(&path).unwrap(),
            fs::metadata(&path).unwrap(),
        ),
    ];
    let expected = quote_portfolio_report::build(Some(&path), Some(db));
    for reopen in 0..2 {
        for (bin, tiny) in [
            (
                env!("CARGO_BIN_EXE_copybot_execution_canary_quote_pnl"),
                false,
            ),
            (env!("CARGO_BIN_EXE_copybot_execution_tiny_economics"), true),
        ] {
            let mut c = Command::new(bin);
            c.args(["--json", "--db-path"])
                .arg(db)
                .args(["--since", cli::TS, "--portfolio-input"])
                .arg(&path);
            if tiny {
                c.arg("--no-live-wallet");
            }
            let out = c.output().unwrap();
            let label = format!("{name}-{}-{reopen}", if tiny { "tiny" } else { "pnl" });
            fs::write(dir.join(format!("{label}.stdout.json")), &out.stdout).unwrap();
            fs::write(dir.join(format!("{label}.stderr")), &out.stderr).unwrap();
            assert!(
                matches!(out.status.code(), Some(0 | 1)),
                "{label}: {:?}",
                out
            );
            fs::write(
                dir.join(format!("{label}.exit")),
                out.status.code().unwrap().to_string(),
            )
            .unwrap();
            let full: Value = serde_json::from_slice(&out.stdout).unwrap();
            assert_eq!(full["portfolio_replay"], expected);
        }
    }
    for (p, bytes, meta) in before {
        assert_eq!(fs::read(p).unwrap(), bytes);
        assert_eq!(
            fs::metadata(p).unwrap().modified().unwrap(),
            meta.modified().unwrap()
        );
        assert_eq!(fs::metadata(p).unwrap().permissions(), meta.permissions());
    }
    expected
}
pub fn refused(v: &Value, event: usize, cash: u64) {
    assert_eq!(v["status"], "replayed", "{v}");
    assert_eq!(v["events"][event]["source_binding"]["state"], "unavailable");
    assert_eq!(
        v["events"][event]["outcome"]["disposition"]["state"],
        "refused"
    );
    assert_eq!(v["book"]["cash_lamports"], cash.to_string());
    assert_eq!(v["book"]["flows"]["expenses"], "0");
    assert_eq!(v["input_coverage"]["kernel_events_complete"], false);
    assert!(v["dataset_net_change_lamports"]["unknown"].is_string());
    assert_eq!(v["production_green"], false);
}
