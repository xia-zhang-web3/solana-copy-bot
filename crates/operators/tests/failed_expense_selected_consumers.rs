#[path = "../../storage-core/tests/common/failed_expense_fixture.rs"]
mod fixture;
use fixture::*;
use serde_json::Value;

#[test]
fn two_existing_readers_preserve_exact_subtotal_and_unknown_coverage() {
    let db = Db::new().unwrap();
    db.store
        .execution_canary_quote_pnl_summary(db.now, db.now - chrono::Duration::hours(1), 10)
        .unwrap();
    db.detect(ORDER, "signature_status").unwrap();
    db.store
        .apply_failed_expense(
            ORDER,
            &db.facts(ORDER, 9_007_199_254_740_993).unwrap(),
            db.now,
        )
        .unwrap();
    db.add("exec-canary:legacy", "old-sig", "buy", db.now)
        .unwrap();
    db.conn().unwrap().execute("UPDATE orders SET status='execution_canary_failed' WHERE order_id='exec-canary:legacy'",[]).unwrap();
    for (binary, pointer) in [
        (
            env!("CARGO_BIN_EXE_copybot_execution_canary_quote_pnl"),
            "/tiny_execution_proof/failed_expenses",
        ),
        (
            env!("CARGO_BIN_EXE_copybot_execution_tiny_economics"),
            "/tiny/failed_expenses",
        ),
    ] {
        let mut command = std::process::Command::new(binary);
        command.args([
            "--db-path",
            db.path.to_str().unwrap(),
            "--json",
            "--limit",
            "1",
        ]);
        if pointer.starts_with("/tiny/") {
            command.arg("--no-live-wallet");
        }
        let out = command.output().unwrap();
        assert!(out.status.success(), "{:?}", out);
        let v: Value = serde_json::from_slice(&out.stdout).unwrap();
        let expense = v.pointer(pointer).unwrap();
        assert_eq!(expense["known_wallet_fee_lamports"], "9007199254740993");
        assert_eq!(expense["unknown_orders"], 1);
        assert_eq!(expense["total_orders"], 2);
        assert_eq!(expense["rows"].as_array().unwrap().len(), 1);
        assert!(expense["cohort_wallet_fee_lamports"].is_null());
        assert!(expense["economic_pnl_lamports"].is_null());
        if pointer.starts_with("/tiny_execution") {
            assert_eq!(v["tiny_execution_quality"]["economic_green"], false);
        } else {
            assert!(v["tiny"]["realized_pnl_sol"].is_null());
        }
    }
}
