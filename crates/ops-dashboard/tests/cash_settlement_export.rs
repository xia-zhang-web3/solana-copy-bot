use anyhow::Result;
use serde_json::{json, Value};
use std::{fs, process::Command};

#[test]
fn dashboard_preserves_exact_cash_strings_zero_and_unknown_economic_basis() -> Result<()> {
    let scratch = std::env::temp_dir().join(format!("cash-export-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(scratch.join("input"))?;
    let result = (|| -> Result<()> {
        for amount in [
            Some("0"),
            Some("-9007199254740993"),
            Some("9007199254740993"),
            None,
        ] {
            fs::write(
                scratch.join("input/execution_canary_quote_pnl.json"),
                serde_json::to_vec(&json!({
                    "as_of":chrono::Utc::now(),"tiny_execution_proof":{"cash_settlements":{
                        "settled_orders":1,"unsettled_orders":0,
                        "known_cash_result_delta_lamports":amount,"cohort_cash_result_delta_lamports":amount
                    }}
                }))?,
            )?;
            let output = Command::new(env!("CARGO_BIN_EXE_copybot_ops_dashboard_snapshot_export"))
                .arg("--input-dir")
                .arg(scratch.join("input"))
                .arg("--output-dir")
                .arg(scratch.join("output"))
                .output()?;
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let report: Value =
                serde_json::from_slice(&fs::read(scratch.join("output/execution.json"))?)?;
            let rows = report["data"]["rows"].as_array().expect("execution rows");
            for label in ["known_cash_result_lamports", "cohort_cash_result_lamports"] {
                let row = rows.iter().find(|r| r[0] == label).unwrap();
                assert_eq!(row[1], amount.unwrap_or("unknown"));
                assert_eq!(row[3], "warning");
            }
            let row = rows.iter().find(|r| r[0] == "economic_pnl").unwrap();
            assert_eq!(row[1], "unknown");
            assert_eq!(row[3], "warning");
        }
        Ok(())
    })();
    fs::remove_dir_all(&scratch)?;
    result
}
