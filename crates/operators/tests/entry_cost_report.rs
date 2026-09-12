#[path = "../../storage-core/tests/common/entry_cost_fixture.rs"]
mod fixture;
#[path = "common/entry_cost_report_fixture.rs"]
mod report_fixture;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use fixture::*;
use report_fixture::*;
use serde_json::Value;
use std::{fs, path::PathBuf, process::Command};

fn config(db: &Db) -> Result<PathBuf> {
    let key = db.dir.path().join("synthetic.json");
    let token = db.dir.path().join("synthetic.token");
    let wallet = write_test_keypair(&key)?;
    fs::write(&token, "synthetic-token")?;
    db.store
        .record_execution_quote_canary_event(&quote_event(db.now))?;
    let path = db.dir.path().join("test.toml");
    fs::write(&path, tiny_config(&db.path, &key, &token, &wallet))?;
    Ok(path)
}
fn actual(db: &Db, cfg: &std::path::Path) -> Result<Value> {
    let out = Command::new(env!("CARGO_BIN_EXE_copybot_execution_canary_quote_pnl"))
        .args(["--config", cfg.to_str().unwrap(), "--json"])
        .output()?;
    assert!(
        out.status.success(),
        "{} {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value = serde_json::from_slice(&out.stdout)?;
    let as_of = DateTime::parse_from_rfc3339(value["as_of"].as_str().unwrap())?.with_timezone(&Utc);
    assert_eq!(
        value["current_entry_cost"],
        serde_json::to_value(db.store.execution_canary_entry_cost(as_of)?)?
    );
    Ok(value)
}
fn check<'a>(v: &'a Value, name: &str) -> &'a Value {
    v["tiny_execution_gate"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == name)
        .unwrap()
}

#[test]
fn entry_cost_actual_primary_json_exact_cap_blocks_only_buy_keeps_legacy_24h() -> Result<()> {
    let db = Db::new()?;
    complete(&db, ORDER, 3)?;
    closed(&db, "loss", Some(-19_999_997), None, "closed", db.now)?;
    let cfg = config(&db)?;
    let before = snapshot(&db)?;
    let v = actual(&db, &cfg)?;
    assert_eq!(v["current_entry_cost"]["known_total_lamports"], "20000000");
    assert_eq!(check(&v, "current_entry_cost_cap")["status"], "block");
    assert_eq!(check(&v, "recent_realized_loss_24h")["status"], "pass");
    assert_eq!(v["tiny_execution_gate"]["can_open_new_tiny_entries"], false);
    assert_eq!(v["tiny_execution_gate"]["can_process_tiny_sells"], true);
    assert_eq!(v["tiny_execution_gate"]["entry_runtime_blocker_count"], 1);
    assert_eq!(v["tiny_execution_gate"]["sell_runtime_blocker_count"], 0);
    assert_eq!(snapshot(&db)?, before);
    // Separate old 24h report still includes the recovery orphan; daily CLOSED excludes it.
    closed(
        &db,
        "recovery-orphan:old-report",
        Some(-30_000_000),
        None,
        "closed",
        db.now - Duration::hours(1),
    )?;
    let v = actual(&db, &cfg)?;
    assert_eq!(v["current_entry_cost"]["known_total_lamports"], "20000000");
    assert_eq!(check(&v, "recent_realized_loss_24h")["status"], "block");
    assert_eq!(v["tiny_execution_gate"]["can_process_tiny_sells"], true);
    Ok(())
}

#[test]
fn entry_cost_actual_primary_unknown_below_cap_keeps_runtime_policy_and_readiness_blocks(
) -> Result<()> {
    for mixed in [false, true] {
        let db = Db::new()?;
        db.detect(ORDER, "signature_status")?;
        if mixed {
            db.add("exec-canary:known", "known-sig", "sell", db.now)?;
            complete(&db, "exec-canary:known", 3)?;
        }
        let cfg = config(&db)?;
        let v = actual(&db, &cfg)?;
        let cost = &v["current_entry_cost"];
        assert_eq!(cost["known_total_lamports"], if mixed { "3" } else { "0" });
        assert!(cost["failed_expenses"]["cohort_wallet_fee_lamports"].is_null());
        assert!(cost["economic_pnl_lamports"].is_null());
        assert_eq!(cost["failed_expenses"]["unknown_orders"], 1);
        assert_eq!(check(&v, "current_entry_cost_cap")["status"], "pass");
        assert_eq!(check(&v, "current_entry_cost_coverage")["status"], "warn");
        assert_eq!(check(&v, "quote_readiness_gate")["status"], "block");
        assert_eq!(
            v["tiny_execution_gate"]["startup_readiness_status"],
            "blocked"
        );
        assert_eq!(v["tiny_execution_gate"]["can_open_new_tiny_entries"], true);
        assert_eq!(v["tiny_execution_gate"]["can_process_tiny_sells"], true);
        assert_eq!(v["tiny_execution_quality"]["economic_green"], false);
        assert!(cost["failed_expenses"]["risk_policy"]
            .as_str()
            .unwrap()
            .contains("known_canary_wallet_fee_subtotal"));
    }
    Ok(())
}

#[path = "../../storage-core/tests/common/entry_cash_fixture.rs"]
mod cash;

#[test]
fn entry_cash_actual_primary_partial_and_full_gross_floor_matches_runtime() -> Result<()> {
    for full in [false, true] {
        let db = Db::new()?;
        complete(&db, ORDER, 3)?;
        cash::inventory(&db.conn()?, "lot", "cash-mint", 3, 9, db.now)?;
        cash::settle(
            &db.store,
            &db.conn()?,
            "exec-canary:cash-a",
            "cash-a",
            "cash-wallet",
            "cash-mint",
            1,
            -4,
            db.now,
        )?;
        if full {
            cash::settle(
                &db.store,
                &db.conn()?,
                "exec-canary:cash-b",
                "cash-b",
                "cash-wallet",
                "cash-mint",
                1,
                6,
                db.now,
            )?;
            cash::settle(
                &db.store,
                &db.conn()?,
                "exec-canary:cash-c",
                "cash-c",
                "cash-wallet",
                "cash-mint",
                1,
                1,
                db.now,
            )?;
        }
        let cfg = config(&db)?;
        let text = fs::read_to_string(&cfg)?
            .replace(
                "canary_max_open_positions = 1",
                "canary_max_open_positions = 10",
            )
            .replace(
                "canary_max_daily_loss_sol = 0.02",
                if full {
                    "canary_max_daily_loss_sol = 0.000000012"
                } else {
                    "canary_max_daily_loss_sol = 0.000000010"
                },
            );
        fs::write(&cfg, text)?;
        let before = snapshot(&db)?;
        let v = actual(&db, &cfg)?;
        let cost = &v["current_entry_cost"];
        assert_eq!(
            cost["closed_loss"]["loss_lamports"],
            if full { "6" } else { "0" }
        );
        assert_eq!(
            cost["cash_loss"]["additional_loss_lamports"],
            if full { "3" } else { "7" }
        );
        assert_eq!(cost["known_total_lamports"], if full { "12" } else { "10" });
        assert_eq!(check(&v, "current_entry_cost_cap")["status"], "block");
        assert!(check(&v, "current_entry_cost_cap")["reason"]
            .as_str()
            .unwrap()
            .contains("uncovered per-position day gross cash loss"));
        assert_eq!(v["tiny_execution_gate"]["can_open_new_tiny_entries"], false);
        assert_eq!(v["tiny_execution_gate"]["can_process_tiny_sells"], true);
        assert_eq!(v["tiny_execution_gate"]["entry_runtime_blocker_count"], 1);
        assert_eq!(v["tiny_execution_quality"]["economic_green"], false);
        // The existing primary is JSON-only; its gate reason/value are the text contract.
        assert_eq!(
            check(&v, "current_entry_cost_cap")["value"],
            cost["known_total_lamports"]
        );
        assert_eq!(snapshot(&db)?, before);
    }
    Ok(())
}

#[test]
fn entry_cash_actual_primary_unavailable_blocks_buy_keeps_partial_components_and_sell() -> Result<()>
{
    for duplicate in [false, true] {
        let db = Db::new()?;
        complete(&db, ORDER, 3)?;
        closed(&db, "closed-floor", Some(-1), None, "closed", db.now)?;
        cash::inventory(&db.conn()?, "lot", "cash-mint", 3, 9, db.now)?;
        cash::settle(
            &db.store,
            &db.conn()?,
            "exec-canary:cash-a",
            "shared",
            "cash-wallet",
            "cash-mint",
            1,
            -4,
            db.now,
        )?;
        if duplicate {
            cash::settle(
                &db.store,
                &db.conn()?,
                "exec-canary:cash-b",
                "shared",
                "cash-wallet",
                "cash-mint",
                1,
                -4,
                db.now,
            )?;
        } else {
            db.conn()?
                .execute("UPDATE fills SET position_id='missing'", [])?;
        }
        let cfg = config(&db)?;
        fs::write(
            &cfg,
            fs::read_to_string(&cfg)?.replace(
                "canary_max_open_positions = 1",
                "canary_max_open_positions = 10",
            ),
        )?;
        let before = snapshot(&db)?;
        let v = actual(&db, &cfg)?;
        let cost = &v["current_entry_cost"];
        assert_eq!(cost["partial_known_subtotal_lamports"], "4");
        assert!(cost["known_total_lamports"].is_null());
        assert!(cost["cash_loss"]["additional_loss_lamports"].is_null());
        assert_eq!(
            cost["cash_loss"]["unavailable_reason"],
            "cash_loss_unavailable"
        );
        assert_eq!(check(&v, "current_entry_cost_cap")["status"], "block");
        assert_eq!(
            check(&v, "current_entry_cost_cap")["reason"],
            "cash_loss_unavailable"
        );
        assert_eq!(check(&v, "current_entry_cost_coverage")["status"], "warn");
        assert_eq!(v["tiny_execution_gate"]["can_open_new_tiny_entries"], false);
        assert_eq!(v["tiny_execution_gate"]["can_process_tiny_sells"], true);
        assert!(cost["economic_pnl_lamports"].is_null());
        assert_eq!(snapshot(&db)?, before);
    }
    Ok(())
}
