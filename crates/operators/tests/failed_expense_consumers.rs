#[path = "../../storage-core/tests/common/failed_expense_fixture.rs"]
mod fixture;
use anyhow::Result;
use fixture::*;
use serde_json::Value;
use std::process::Command;

#[test]
fn existing_primary_economics_readiness_preserve_exact_failed_costs_without_economic_green(
) -> Result<()> {
    let db = Db::new()?;
    db.store
        .execution_canary_quote_pnl_summary(db.now, db.now - chrono::Duration::hours(1), 10)?;
    db.detect(ORDER, "signature_status")?;
    db.store
        .apply_failed_expense(ORDER, &db.facts(ORDER, 9_007_199_254_740_993)?, db.now)?;
    db.add("exec-canary:legacy", "old-sig", "buy", db.now)?;
    db.conn()?.execute(
        "UPDATE orders SET status='execution_canary_failed' WHERE order_id='exec-canary:legacy'",
        [],
    )?;
    for (binary, pointer) in [
        (
            env!("CARGO_BIN_EXE_copybot_execution_canary_quote_pnl"),
            "/tiny_execution_proof/failed_expenses",
        ),
        (
            env!("CARGO_BIN_EXE_copybot_execution_tiny_economics"),
            "/tiny/failed_expenses",
        ),
        (
            env!("CARGO_BIN_EXE_copybot_execution_canary_readiness"),
            "/failed_expenses",
        ),
    ] {
        let out = Command::new(binary)
            .args([
                "--db-path",
                db.path.to_str().unwrap(),
                "--json",
                "--limit",
                "1",
            ])
            .output()?;
        assert!(
            out.status.success(),
            "{} {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let value: Value = serde_json::from_slice(&out.stdout)?;
        let expense = value.pointer(pointer).expect("existing consumer expense");
        assert_eq!(expense["known_wallet_fee_lamports"], "9007199254740993");
        assert_eq!(expense["unknown_orders"], 1);
        assert_eq!(expense["rows"].as_array().unwrap().len(), 1);
        assert_eq!(expense["total_orders"], 2);
        assert!(expense["cohort_wallet_fee_lamports"].is_null());
        assert!(expense["economic_pnl_lamports"].is_null());
        if pointer.starts_with("/tiny_execution") {
            assert_eq!(value["tiny_execution_quality"]["economic_green"], false);
            assert_ne!(value["tiny_execution_quality"]["verdict"], "healthy");
        }
        if pointer == "/failed_expenses" {
            assert_eq!(value["economic_green"], false);
            assert_eq!(value["production_green"], false);
            assert_eq!(
                value["readiness_basis"],
                "quote_simulation_only_not_economic"
            );
        }
        if pointer.starts_with("/tiny/") {
            assert!(value["tiny"]["realized_pnl_sol"].is_null());
        }
    }
    Ok(())
}
