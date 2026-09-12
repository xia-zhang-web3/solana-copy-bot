#[path = "common/entry_cost_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use fixture::*;
use rusqlite::params;
use serde_json::json;

#[test]
fn entry_cost_window_uses_parsed_order_nanoseconds_and_half_open_utc_day() -> Result<()> {
    let db = fixed()?;
    let end = time("2026-09-06T00:00:00.000000200Z");
    for (name, at) in [
        ("before", "2026-09-05T23:59:59.999999999Z"),
        ("start", "2026-09-06T00:00:00Z"),
        ("inside", "2026-09-06T00:00:00.000000199Z"),
        ("end", "2026-09-06T00:00:00.000000200Z"),
        ("after", "2026-09-06T00:00:00.000000201Z"),
    ] {
        let id = format!("exec-canary:{name}");
        db.add(&id, name, "sell", time(at))?;
        complete(&db, &id, 1)?;
    }
    let value = db.store.execution_canary_entry_cost(end)?;
    assert_eq!(value.known_total_lamports.as_deref().unwrap(), "2");
    assert_eq!(value.failed_expenses.total_orders, 2);
    assert!(value.failed_expenses.window_basis.contains("parsed_UTC"));
    let inside = "exec-canary:inside";
    for table in ["orders", "execution_failed_expense_tasks"] {
        let column = if table == "orders" {
            "submit_ts"
        } else {
            "operation_at"
        };
        db.conn()?.execute(
            &format!("UPDATE {table} SET {column}=?2 WHERE order_id=?1"),
            params![inside, "2026-09-06T01:00:00.000000199+01:00"],
        )?;
    }
    assert_eq!(
        db.store
            .execution_canary_entry_cost(end)?
            .known_total_lamports
            .as_deref()
            .unwrap(),
        "2"
    );
    Ok(())
}

#[test]
fn entry_cost_task_date_cannot_hide_todays_broken_order_binding() -> Result<()> {
    let db = fixed()?;
    complete(&db, ORDER, 20_000_000)?;
    db.conn()?.execute("UPDATE execution_failed_expense_tasks SET operation_at='2026-09-05T12:00:00+00:00' WHERE order_id=?1", [ORDER])?;
    let value = cost(&db)?;
    assert_eq!(value.failed_expenses.total_orders, 1);
    assert_eq!(value.failed_expenses.unknown_orders, 1);
    assert!(value.failed_expenses.known_wallet_fee_lamports.is_none());
    assert_eq!(value.failed_expenses.coverage, "partial_unresolved");
    // Public report intentionally retains its historical task-first cohort selection.
    assert_eq!(db.report(10)?.total_orders, 0);
    Ok(())
}

#[test]
fn entry_cost_all_canary_routes_wallets_and_only_canary_task_ids() -> Result<()> {
    let db = fixed()?;
    complete(&db, ORDER, 2)?;
    let id = "exec-canary:other-wallet-route";
    db.add(id, "other-signature", "buy", db.now)?;
    db.conn()?.execute(
        "UPDATE orders SET route='other-route' WHERE order_id=?1",
        [id],
    )?;
    db.store.detect_failed_expense(
        id,
        "other-wallet",
        "signature_status",
        "confirmed",
        Some(42),
        &json!({"InstructionError":[0,{"Custom":7}]}),
        db.now,
    )?;
    let mut facts = db.facts(id, 3)?;
    facts.wallet = "other-wallet".into();
    facts.payer = Some("other-wallet".into());
    db.store.apply_failed_expense(id, &facts, db.now)?;
    assert_eq!(cost(&db)?.known_total_lamports.as_deref().unwrap(), "5");
    let conn = db.conn()?;
    conn.execute_batch("PRAGMA foreign_keys=OFF")?;
    // Fixture-only legacy noncanary task: the public report admits tasks with any ID.
    for table in [
        "orders",
        "execution_failed_expense_tasks",
        "execution_failed_expense_facts",
        "execution_failed_expense_ledger",
    ] {
        conn.execute(
            &format!("UPDATE {table} SET order_id='operator:legacy-failure' WHERE order_id=?1"),
            [id],
        )?;
    }
    assert_eq!(cost(&db)?.known_total_lamports.as_deref().unwrap(), "2");
    assert_eq!(
        db.report(10)?.known_wallet_fee_lamports.as_deref(),
        Some("5")
    );
    assert_eq!(db.report(10)?.total_orders, 2);
    Ok(())
}

#[test]
fn entry_cost_enrichment_keeps_original_submit_window_and_never_changes_money() -> Result<()> {
    let mut db = fixed()?;
    db.detect(ORDER, "signature_status")?;
    assert!(cost(&db)?
        .failed_expenses
        .known_wallet_fee_lamports
        .is_none());
    db.store
        .apply_failed_expense(ORDER, &db.facts(ORDER, 7)?, db.now + Duration::days(2))?;
    let after = snapshot(&db)?;
    for _ in 0..2 {
        db.reopen()?;
        assert_eq!(cost(&db)?.known_total_lamports.as_deref().unwrap(), "7");
        let later = db
            .store
            .execution_canary_entry_cost(db.now + Duration::days(2))?;
        assert_eq!(later.known_total_lamports.as_deref().unwrap(), "0");
        assert_eq!(later.failed_expenses.coverage, "empty_unknown");
        assert!(later.failed_expenses.cohort_wallet_fee_lamports.is_none());
        assert_eq!(snapshot(&db)?, after);
    }
    assert_eq!(db.count("execution_failed_expense_ledger")?, 1);
    Ok(())
}

#[test]
fn entry_cost_closed_scope_excludes_open_and_orphans_but_keeps_real_orphan_fee() -> Result<()> {
    let db = fixed()?;
    closed(&db, "normal", Some(-5), None, "closed", db.now)?;
    closed(&db, "profit", Some(8), None, "closed", db.now)?;
    closed(&db, "open", Some(-10), None, "open", db.now)?;
    closed(
        &db,
        "recovery-orphan:synthetic",
        Some(-1_000_000_000),
        None,
        "closed",
        db.now,
    )?;
    let fee = "exec-canary:recovery-orphan:real-failed-sell";
    db.add(fee, "orphan-fee", "sell", db.now)?;
    complete(&db, fee, 3)?;
    let value = cost(&db)?;
    assert_eq!(value.closed_loss.loss_lamports, "5");
    assert_eq!(value.known_total_lamports.as_deref().unwrap(), "8");
    assert!(db.store.execution_canary_realized_loss_sol_since(db.now)? > 1.0);
    // The old CLOSED cutoff intentionally has no as_of upper bound.
    closed(
        &db,
        "future-closed",
        Some(-2),
        None,
        "closed",
        db.now + Duration::days(1),
    )?;
    closed(
        &db,
        "past-closed",
        Some(-100),
        None,
        "closed",
        db.now - Duration::days(1),
    )?;
    assert_eq!(cost(&db)?.known_total_lamports.as_deref().unwrap(), "10");
    Ok(())
}
