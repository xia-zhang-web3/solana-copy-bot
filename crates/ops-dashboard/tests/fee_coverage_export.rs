use anyhow::Result;
use serde_json::{json, Value};
use std::{fs, path::PathBuf, process::Command};

struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn exported_unknown_net_is_unknown_and_warns_while_zero_is_known() -> Result<()> {
    let scratch =
        Scratch(std::env::temp_dir().join(format!("fee-export-{}", uuid::Uuid::new_v4())));
    let input = scratch.0.join("input");
    let output = scratch.0.join("output");
    fs::create_dir_all(&input)?;
    for (net, display, level) in [
        (Value::Null, "unknown", "warning"),
        (json!(0.0), "0.000000 SOL", "safe"),
    ] {
        fs::write(
            input.join("execution_canary_quote_pnl.json"),
            serde_json::to_vec(&json!({
                "as_of": chrono::Utc::now(),
                "summary": {
                    "shadow_pnl_sol": 0.003,
                    "quote_adjusted_pnl_after_priority_fee_sol": net,
                    "quote_after_fee_vs_shadow_delta_sol": net
                }
            }))?,
        )?;
        let result = Command::new(env!("CARGO_BIN_EXE_copybot_ops_dashboard_snapshot_export"))
            .arg("--input-dir")
            .arg(&input)
            .arg("--output-dir")
            .arg(&output)
            .output()?;
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let snapshot: Value = serde_json::from_slice(&fs::read(output.join("strategy.json"))?)?;
        for label in ["quote_after_priority", "quote_vs_shadow_delta"] {
            let row = snapshot["data"]["rows"]
                .as_array()
                .expect("rows")
                .iter()
                .find(|row| row[0] == label)
                .expect("PnL row");
            assert_eq!(row[1], display);
            assert_eq!(row[3], level);
        }
    }
    Ok(())
}
