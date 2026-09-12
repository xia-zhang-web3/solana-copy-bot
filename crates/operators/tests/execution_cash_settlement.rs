#[path = "../../storage-core/tests/common/sell_settlement_fixture.rs"]
mod fixture;
use anyhow::Result;
use fixture::*;
use serde_json::Value;
use std::process::Command;

#[test]
fn existing_operator_json_exposes_owned_only_cash_with_unknown_economics() -> Result<()> {
    for native in [-13, 0, 29] {
        let db = Db::new(7, 23, 0, 7, native)?;
        // Existing quote report schemas include compatibility columns initialized by
        // their writable storage API before a read-only operator opens the DB.
        db.store.execution_canary_quote_pnl_summary(
            db.now,
            db.now - chrono::Duration::hours(1),
            10,
        )?;
        db.store
            .apply_execution_canary_sell_settlement(&db.facts(7, native), db.now)?;
        for (binary, field) in [
            (
                env!("CARGO_BIN_EXE_copybot_execution_canary_quote_pnl"),
                "tiny_execution_proof",
            ),
            (
                env!("CARGO_BIN_EXE_copybot_execution_tiny_economics"),
                "tiny",
            ),
        ] {
            let output = Command::new(binary)
                .arg("--db-path")
                .arg(&db.path)
                .arg("--json")
                .output()?;
            assert!(
                output.status.success(),
                "{} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let report: Value = serde_json::from_slice(&output.stdout)?;
            assert!(report["error"].is_null(), "{report}");
            let cash = &report[field]["cash_settlements"];
            assert_eq!(cash["settled_orders"], 1);
            assert_eq!(
                cash["cohort_cash_result_delta_lamports"],
                (native - 23).to_string()
            );
            assert_eq!(cash["rows"][0]["order_id"], ORDER);
            assert_eq!(
                cash["rows"][0]["wallet_native_cash_delta_lamports"],
                native.to_string()
            );
            assert!(cash["rows"][0]["swap_price_sol"].is_null());
            assert!(cash["economic_pnl_sol"].is_null());
            if field == "tiny" {
                assert!(report[field]["realized_pnl_sol"].is_null());
                assert_eq!(report[field]["economic_pnl_basis"], "unresolved");
            } else {
                assert!(report[field]["summary"]["tiny_realized_pnl_sol"].is_null());
                assert_ne!(report["tiny_execution_quality"]["verdict"], "healthy");
                assert_eq!(report["tiny_execution_quality"]["cash_settled_orders"], 1);
            }
        }
    }
    Ok(())
}
