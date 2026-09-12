#[path = "common/sell_settlement_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::HistoryRetentionCutoffs;
use fixture::*;

#[test]
fn owned_only_report_has_all_cash_signs_without_economic_price_or_shadow_doublecount() -> Result<()>
{
    for native in [-13, 0, 29] {
        let db = Db::new(7, 23, 0, 7, native)?;
        db.store
            .apply_execution_canary_sell_settlement(&db.facts(7, native), db.now)?;
        let report = db.store.execution_tiny_proof_report(
            db.now + Duration::seconds(1),
            db.now - Duration::seconds(1),
            10,
        )?;
        assert!(report.trades.is_empty(), "no shadow close exists");
        assert!(report.summary.tiny_realized_pnl_sol.is_none());
        let cash = &report.cash_settlements;
        assert_eq!(cash.total_sell_orders, 1);
        assert_eq!(cash.settled_orders, 1);
        assert_eq!(
            cash.known_wallet_native_delta_lamports,
            Some(native.to_string())
        );
        assert_eq!(
            cash.cohort_cash_result_delta_lamports,
            Some((native - 23).to_string())
        );
        let row = &cash.rows[0];
        assert_eq!(row.order_id, ORDER);
        assert_eq!(row.sold_raw.as_deref(), Some("7"));
        assert!(row.swap_price_sol.is_none());
        assert!(row.economic_pnl_sol.is_none());
        assert!(row.transaction_fee_lamports.is_none());
        assert_eq!(row.fee_coverage, "missing");
        assert!(cash.economic_pnl_sol.is_none());
        let json = serde_json::to_value(&report)?;
        assert!(json["cash_settlements"]["rows"][0]["swap_price_sol"].is_null());
        assert_eq!(
            json["cash_settlements"]["rows"][0]["wallet_native_cash_delta_lamports"],
            native.to_string()
        );
        // The cash ledger is order-driven even when shadow joins fan out.
        for _ in 0..2 {
            db.conn()?.execute("INSERT INTO shadow_closed_trades(signal_id,wallet_id,token,qty,entry_cost_sol,exit_value_sol,pnl_sol,opened_ts,closed_ts)
                VALUES('sell-signal','leader','mint',7,999,999,0,?1,?1)",[db.now.to_rfc3339()])?;
        }
        let duplicate = db.store.execution_tiny_proof_report(
            db.now + Duration::seconds(1),
            db.now - Duration::seconds(1),
            10,
        )?;
        assert_eq!(&duplicate.cash_settlements, cash);
    }
    Ok(())
}

#[test]
fn mixed_and_unsettled_cohorts_keep_coverage_unknown_and_limits_do_not_change_totals() -> Result<()>
{
    let db = Db::new(7, 23, 0, 7, 23)?;
    let since = db.now - Duration::seconds(1);
    let as_of = db.now + Duration::seconds(1);
    let pending = db
        .store
        .execution_cash_settlement_report(since, as_of, 10)?;
    assert_eq!(pending.unsettled_orders, 1);
    assert!(pending.known_cash_result_delta_lamports.is_none());
    let empty =
        db.store
            .execution_cash_settlement_report(as_of, as_of + Duration::seconds(1), 10)?;
    assert_eq!(empty.total_sell_orders, 0);
    assert!(empty.cohort_cash_result_delta_lamports.is_none());
    db.store
        .apply_execution_canary_sell_settlement(&db.facts(7, 23), db.now)?;
    let known = db.store.execution_cash_settlement_report(since, as_of, 0)?;
    assert!(known.rows.is_empty());
    assert!(known.rows_truncated);
    assert_eq!(
        known.cohort_cash_result_delta_lamports.as_deref(),
        Some("0")
    );
    db.conn()?.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,attempt)
        VALUES('exec-canary:legacy','sell-signal','tiny',?1,'execution_canary_confirmed','legacy',1)",[db.now.to_rfc3339()])?;
    db.conn()?.execute(
        "INSERT INTO fills(order_id,token,qty,avg_price,fee,slippage_bps)
        VALUES('exec-canary:legacy','mint',1,0.1,0,0)",
        [],
    )?;
    db.conn()?.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,attempt)
        VALUES('exec-canary:unsettled','sell-signal','tiny',?1,'execution_canary_confirmed_unreconciled','pending',1)",[db.now.to_rfc3339()])?;
    let mixed = db.store.execution_cash_settlement_report(since, as_of, 1)?;
    assert_eq!(
        (
            mixed.total_sell_orders,
            mixed.settled_orders,
            mixed.legacy_fill_orders,
            mixed.unsettled_orders
        ),
        (3, 1, 1, 1)
    );
    assert_eq!(mixed.known_cash_result_delta_lamports.as_deref(), Some("0"));
    assert!(mixed.cohort_cash_result_delta_lamports.is_none());
    assert!(mixed.economic_pnl_sol.is_none());
    assert!(mixed.rows_truncated);
    Ok(())
}

#[test]
fn closed_cash_loss_is_visible_to_existing_cap_and_survives_retention_helpers() -> Result<()> {
    let db = Db::new(7, 900_000_000, 0, 7, -300_000_000)?;
    db.store
        .apply_execution_canary_sell_settlement(&db.facts(7, -300_000_000), db.now)?;
    assert!(
        (db.store
            .execution_canary_realized_loss_sol_since(db.now - Duration::seconds(1))?
            - 1.2)
            .abs()
            < 1e-12
    );
    db.store.ensure_history_retention_tables()?;
    let cutoff = db.now + Duration::days(10);
    db.store.apply_history_retention(
        HistoryRetentionCutoffs {
            risk_events_before: cutoff,
            copy_signals_before: cutoff,
            orders_before: cutoff,
            shadow_closed_trades_before: cutoff,
            execution_quote_canary_before: cutoff,
        },
        true,
    )?;
    assert!(db.store.execution_canary_fill_exists(ORDER)?);
    db.store
        .validate_execution_canary_cash_settlement_replay(ORDER, "wallet")?;
    assert_eq!(
        db.store
            .execution_cash_settlement_report(db.now - Duration::seconds(1), cutoff, 10)?
            .settled_orders,
        1
    );
    Ok(())
}

#[test]
fn missing_signal_does_not_drop_unsupported_receipt_from_cash_cohort() -> Result<()> {
    let db = Db::new(7, 23, 0, 7, 0)?;
    // Deliberately simulate incomplete legacy/imported data; normal FK enforcement
    // already prevents deleting a signal referenced by an execution order.
    db.conn()?.execute_batch(
        "PRAGMA foreign_keys=OFF; DELETE FROM copy_signals WHERE signal_id='sell-signal';",
    )?;
    let report =
        db.store
            .execution_cash_settlement_report(db.now - Duration::seconds(1), db.now, 10)?;
    assert_eq!(report.total_sell_orders, 1);
    assert_eq!(report.unsettled_orders, 1);
    assert_eq!(report.rows[0].order_id, ORDER);
    assert_eq!(report.rows[0].token, "mint");
    assert!(report.cohort_cash_result_delta_lamports.is_none());
    // A broken identity on an already completed settlement fails the report,
    // rather than silently dropping the fill and reporting a complete cohort.
    let db = Db::new(7, 23, 0, 7, 0)?;
    db.store
        .apply_execution_canary_sell_settlement(&db.facts(7, 0), db.now)?;
    // Deliberately simulate incomplete legacy/imported data; normal FK enforcement
    // already prevents deleting a signal referenced by an execution order.
    db.conn()?.execute_batch(
        "PRAGMA foreign_keys=OFF; DELETE FROM copy_signals WHERE signal_id='sell-signal';",
    )?;
    assert!(db
        .store
        .execution_cash_settlement_report(db.now - Duration::seconds(1), db.now, 10)
        .is_err());
    Ok(())
}
