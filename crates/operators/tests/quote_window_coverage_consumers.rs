#[path = "../../storage-core/tests/common/quote_allocation_fixture.rs"]
mod fixture;
#[path = "../../storage-core/tests/common/quote_skipped_ready_fixture.rs"]
mod ready_fixture;
#[path = "../../storage-core/tests/common/quote_allocation_sql.rs"]
mod sql;
use anyhow::Result;
use chrono::{Duration, Utc};
use fixture::Fixture;
use serde_json::Value;
use std::process::{Command, Stdio};

const OPERATORS: [(&str, &str); 2] = [
    (
        env!("CARGO_BIN_EXE_copybot_execution_canary_quote_pnl"),
        "summary",
    ),
    (
        env!("CARGO_BIN_EXE_copybot_execution_tiny_economics"),
        "canary",
    ),
];

fn invoke(f: &Fixture, binary: &str, limit: Option<u32>) -> Result<(bool, Value)> {
    let mut command = Command::new(binary);
    command
        .arg("--db-path")
        .arg(f.dir.path().join("allocation.db"))
        .args(["--json", "--since", &f.opened.to_rfc3339()]);
    if let Some(limit) = limit {
        command.arg("--limit").arg(limit.to_string());
    }
    let output = command.stdin(Stdio::null()).output()?;
    let value = serde_json::from_slice(&output.stdout)?;
    Ok((output.status.success(), value))
}

#[test]
fn actual_default_and_explicit_limits_expose_sample_coverage_and_preserve_nullable_money(
) -> Result<()> {
    let f = ready_fixture::ready_mixed(Utc::now() - Duration::hours(1))?;
    for state in ["known", "missing", "zero"] {
        match state {
            "missing" => f.sql("UPDATE execution_quote_canary_events SET quote_status='error' WHERE signal_id='sell-1'")?,
            "zero" => f.sql("UPDATE execution_quote_canary_events SET quote_status='ok',quote_out_amount_raw='100000000' WHERE side='sell'")?,
            _ => {}
        }
        for (binary, section) in OPERATORS {
            for limit in [None, Some(30)] {
                let (success, value) = invoke(&f, binary, limit)?;
                assert!(success, "{value}");
                println!(
                    "window_cli_observation={}",
                    serde_json::json!({"section":section,"state":state,"limit":limit,"report":value})
                );
                let report = &value[section];
                let sampled = if limit.is_some() { 30 } else { 31 };
                assert_eq!(report["window_total_closed_trades"], 31);
                assert_eq!(report["sampled_closed_trades"], sampled);
                assert_eq!(report["omitted_closed_trades"], 31 - sampled);
                assert_eq!(report["financial_totals_scope"], "selected_market_closes");
                assert_eq!(report["skipped_trades"], 1);
                let net = &report["quote_adjusted_pnl_after_priority_fee_sol"];
                if state == "missing" && limit.is_none() {
                    assert!(net.is_null());
                    assert_eq!(report["unknown_trades"], 1);
                } else {
                    assert!(net.is_number());
                    assert_eq!(report["unknown_trades"], 0);
                    let expected = if state == "zero" {
                        0.0
                    } else if limit.is_some() {
                        0.29
                    } else {
                        0.3
                    };
                    assert!((net.as_f64().unwrap() - expected).abs() < 1e-12);
                }
                if section == "summary" {
                    assert_eq!(report["total_closed_trades"], sampled);
                    assert_eq!(report["trades"].as_array().unwrap().len(), sampled as usize);
                    let check = report["readiness_gate"]["checks"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|c| c["name"] == "quote_window_coverage")
                        .unwrap();
                    assert_eq!(
                        check["status"],
                        if limit.is_some() { "block" } else { "pass" }
                    );
                    assert_eq!(
                        report["readiness_gate"]["can_start_tiny_execution"],
                        state == "known" && limit.is_none()
                    );
                } else {
                    assert_eq!(
                        value["shadow"]["market_totals_scope"],
                        "selected_market_closes"
                    );
                    assert_eq!(value["shadow"]["market_trades"], sampled);
                    assert!(value["tiny"]["realized_pnl_sol"].is_null());
                }
            }
        }
    }
    Ok(())
}

#[test]
fn unreadable_window_evidence_fails_both_operators_instead_of_zero_coverage() -> Result<()> {
    let f = Fixture::new()?;
    f.sql("ALTER TABLE shadow_closed_trades RENAME TO unavailable_closed_trades")?;
    for (binary, section) in OPERATORS {
        let (success, value) = invoke(&f, binary, None)?;
        assert!(!success, "{value}");
        assert!(value[section].is_null());
        assert!(value["error"].is_string());
    }
    Ok(())
}
