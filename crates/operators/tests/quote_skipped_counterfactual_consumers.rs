#[path = "../../storage-core/tests/common/quote_allocation_fixture.rs"]
mod fixture;
#[path = "../../storage-core/tests/common/quote_skipped_ready_fixture.rs"]
mod ready_fixture;
#[path = "../../storage-core/tests/common/quote_allocation_sql.rs"]
mod sql;
use anyhow::Result;
use chrono::{Duration, Utc};
use serde_json::Value;
use std::process::{Command, Stdio};

#[test]
fn primary_and_economics_json_separate_skipped_decision_from_unknown_counterfactual() -> Result<()>
{
    let f = ready_fixture::ready_mixed(Utc::now() - Duration::hours(1))?;
    f.sql("DELETE FROM execution_quote_canary_events WHERE signal_id='sell-31'")?;
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
            .args([
                "--json",
                "--since",
                &f.opened.to_rfc3339(),
                "--limit",
                "100",
            ])
            .stdin(Stdio::null())
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let json: Value = serde_json::from_slice(&output.stdout)?;
        let view = &json[section];
        println!("operator_{section}_observation={json}");
        assert_eq!(view["skipped_trades"], 1);
        assert_eq!(view["unknown_trades"], 0);
        assert_eq!(view["skipped_counterfactual_gross_known_trades"], 0);
        assert_eq!(view["skipped_counterfactual_gross_unknown_trades"], 1);
        assert_eq!(view["skipped_counterfactual_net_known_trades"], 0);
        assert_eq!(view["skipped_counterfactual_net_unknown_trades"], 1);
        assert!(view["skipped_counterfactual_pnl_sol"].is_null());
        assert!(view["skipped_counterfactual_pnl_after_priority_fee_sol"].is_null());
        assert!(view["quote_adjusted_pnl_after_priority_fee_sol"].is_null());
        assert!((view["quote_adjusted_pnl_sol"].as_f64().unwrap() - 0.3).abs() < 1e-12);
        assert_eq!(view["quote_win_count"], 30);
        assert_eq!(view["quote_loss_count"], 0);
        if section == "summary" {
            assert_eq!(view["total_closed_trades"], 31);
            assert_eq!(view["pnl_counted_trades"], 30);
            assert_eq!(view["trades"].as_array().unwrap().len(), 31);
            let skipped = &view["trades"][0];
            assert_eq!(skipped["status"], "would_skip");
            assert_eq!(skipped["reason"], "entry_decision:would_skip");
            assert_eq!(
                skipped["skipped_counterfactual_reason"],
                "missing_exit_quote"
            );
            assert!(skipped["entry_cost_sol"].is_null());
            assert!(skipped["exit_quote_sol"].is_null());
            assert_eq!(view["readiness_gate"]["can_start_tiny_execution"], false);
            assert!(view["readiness_gate"]["checks"]
                .as_array()
                .unwrap()
                .iter()
                .any(|c| c["name"] == "skipped_counterfactual_coverage" && c["status"] == "block"));
        } else {
            assert_eq!(view["counted_trades"], 30);
            assert_eq!(json["shadow"]["market_trades"], 31);
            assert!(json["tiny"]["realized_pnl_sol"].is_null());
            assert_eq!(json["tiny"]["economic_pnl_basis"], "unresolved");
        }
    }
    assert_eq!(
        f.store.executable_wallet_feedback_since(f.opened)?["wallet"].samples,
        30
    );
    Ok(())
}

#[test]
fn existing_operator_json_only_contract_does_not_gain_a_false_text_success() -> Result<()> {
    let f = fixture::Fixture::new()?;
    for (binary, reason) in [
        (
            env!("CARGO_BIN_EXE_copybot_execution_canary_quote_pnl"),
            "execution_canary_quote_pnl_json_required",
        ),
        (
            env!("CARGO_BIN_EXE_copybot_execution_tiny_economics"),
            "execution_tiny_economics_error",
        ),
    ] {
        let output = Command::new(binary)
            .arg("--db-path")
            .arg(f.dir.path().join("allocation.db"))
            .stdin(Stdio::null())
            .output()?;
        assert!(!output.status.success());
        let json: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(json["reason_class"], reason);
        assert!(json["error"]
            .as_str()
            .unwrap()
            .contains("--json is required"));
    }
    Ok(())
}
