use anyhow::Result;
use serde_json::{json, Value};
use std::{fs, process::Command};
#[test]
fn dashboard_exports_failed_fee_coverage_exact_money_and_missing_values() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("failed-expense-export-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(dir.join("in"))?;
    let result = (|| -> Result<()> {
        for value in [
            json!("0"),
            json!("9007199254740993"),
            json!("18446744073709551615"),
            json!(null),
            json!(9007199254740992_u64),
        ] {
            fs::write(
                dir.join("in/execution_canary_quote_pnl.json"),
                serde_json::to_vec(
                    &json!({"as_of":chrono::Utc::now(),"tiny_execution_proof":{"failed_expenses":{
                "known_wallet_fee_lamports":value,"cohort_wallet_fee_lamports":null,"total_orders":3,"unknown_orders":1,"unresolved_orders":2,"legacy_uncovered_orders":1,"known_native_delta_lamports":"-9007199254740993","known_unexplained_delta_lamports":"-1","coverage":"partial_unresolved","source_basis":"failed_getTransaction_confirmed_or_finalized","since":"2026-09-05T00:00:00Z","as_of":"2026-09-05T01:00:00Z","history_coverage":"unverified_prior_history_no_backfill"}}}),
                )?,
            )?;
            let out = Command::new(env!("CARGO_BIN_EXE_copybot_ops_dashboard_snapshot_export"))
                .arg("--input-dir")
                .arg(dir.join("in"))
                .arg("--output-dir")
                .arg(dir.join("out"))
                .output()?;
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            let export: Value = serde_json::from_slice(&fs::read(dir.join("out/execution.json"))?)?;
            let rows = export["data"]["rows"].as_array().unwrap();
            for (key, expected) in [
                (
                    "failed_expense_known_lamports",
                    value.as_str().unwrap_or("unknown"),
                ),
                ("failed_expense_unknown_orders", "1"),
                ("failed_expense_unresolved_orders", "2"),
                ("failed_expense_native_delta_lamports", "-9007199254740993"),
                ("failed_expense_unexplained_lamports", "-1"),
                ("failed_expense_cohort_lamports", "unknown"),
            ] {
                assert_eq!(rows.iter().find(|r| r[0] == key).unwrap()[1], expected);
            }
        }
        Ok(())
    })();
    fs::remove_dir_all(dir)?;
    result
}
