#[path = "../../storage-core/tests/common/quote_allocation_fixture.rs"]
mod fixture;
#[path = "../../storage-core/tests/common/quote_allocation_sql.rs"]
mod sql;
use anyhow::Result;
use chrono::{Duration, Utc};
use fixture::Fixture;
use serde_json::Value;
use std::process::Command;

#[test]
fn primary_and_economics_actual_cli_keep_allocated_fees_and_unknown_cash() -> Result<()> {
    let mut f = Fixture::new()?;
    f.opened = Utc::now() - Duration::hours(1);
    f.buy("100", Some(7))?;
    f.exit(1, "25", Some(2))?;
    f.exit(2, "75", Some(2))?;
    for (binary, section) in [
        (
            env!("CARGO_BIN_EXE_copybot_execution_canary_quote_pnl"),
            "summary",
        ),
        (
            env!("CARGO_BIN_EXE_copybot_execution_tiny_economics"),
            "canary",
        ),
    ] {
        for (since, limit, net) in [(0, 100, 0.12 - 11e-9), (2, 1, 0.035 - 7e-9)] {
            let output = Command::new(binary)
                .arg("--db-path")
                .arg(f.dir.path().join("allocation.db"))
                .args([
                    "--json",
                    "--since",
                    &(f.opened + Duration::seconds(since)).to_rfc3339(),
                    "--limit",
                    &limit.to_string(),
                ])
                .output()?;
            let value: Value = serde_json::from_slice(&output.stdout)?;
            assert!(output.status.success(), "{value}");
            assert!(
                (value[section]["quote_adjusted_pnl_after_priority_fee_sol"]
                    .as_f64()
                    .unwrap()
                    - net)
                    .abs()
                    < 1e-12,
                "{value}"
            );
            if section == "summary" {
                assert_eq!(
                    value[section]["trades"][0]["fee_allocation"]["buy_fee_allocated_lamports"],
                    "5"
                );
                assert_eq!(
                    value[section]["trades"][0]["priority_fee_lamports_total"],
                    7
                );
                assert_eq!(
                    value[section]["trades"][0]["fee_allocation"]["observed_buy_fee_lamports"],
                    "7"
                );
            } else {
                assert!(value["tiny"]["realized_pnl_sol"].is_null());
                assert_eq!(value["tiny"]["economic_pnl_basis"], "unresolved");
            }
        }
    }
    f.sql("DELETE FROM execution_quote_canary_events WHERE signal_id='sell-1'")?;
    for (binary, section) in [
        (
            env!("CARGO_BIN_EXE_copybot_execution_canary_quote_pnl"),
            "summary",
        ),
        (
            env!("CARGO_BIN_EXE_copybot_execution_tiny_economics"),
            "canary",
        ),
    ] {
        let output = Command::new(binary)
            .arg("--db-path")
            .arg(f.dir.path().join("allocation.db"))
            .args(["--json", "--since", &f.opened.to_rfc3339(), "--limit", "1"])
            .output()?;
        let value: Value = serde_json::from_slice(&output.stdout)?;
        assert!(output.status.success(), "{value}");
        assert!(
            value[section]["quote_adjusted_pnl_after_priority_fee_sol"].is_null(),
            "{value}"
        );
        if section == "summary" {
            assert!(
                !value[section]["readiness_gate"]["can_start_tiny_execution"]
                    .as_bool()
                    .unwrap()
            );
        }
    }
    Ok(())
}
