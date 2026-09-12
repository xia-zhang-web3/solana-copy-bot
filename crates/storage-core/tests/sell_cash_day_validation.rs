#[path = "common/sell_cash_day_fixture.rs"]
mod fixture;
use anyhow::{Context, Result};
use chrono::Duration;
use copybot_storage_core::*;
use fixture::*;

fn rejected_unchanged(db: &Db) -> Result<()> {
    let before = snapshot(&db.conn()?)?;
    let ro = SqliteStore::open_read_only(&db.path)?;
    assert!(ro.execution_canary_sell_cash_day(as_of()).is_err());
    assert_eq!(snapshot(&db.conn()?)?, before);
    Ok(())
}

#[test]
fn invalid_dates_even_outside_the_day_never_disappear() -> Result<()> {
    for at in [
        "not-a-date",
        "2026-02-30T12:00:00Z",
        "2026-09-06T12:00:00",
        "",
        "2026-09-06T00:00:00+25:00",
    ] {
        let db = Db::new(1, 3, 0, 1, 0)?;
        first(&db, as_of() - Duration::days(5))?;
        db.conn()?
            .execute("UPDATE fills SET settlement_ts=?1", [at])?;
        rejected_unchanged(&db)?;
    }
    Ok(())
}

#[test]
fn exact_claim_identity_arithmetic_and_completion_corruption_is_an_error() -> Result<()> {
    for sql in [
        "UPDATE orders SET tx_signature='wrong'",
        "UPDATE execution_canary_receipt_proofs SET wallet_pubkey='wrong'",
        "UPDATE execution_canary_receipt_proofs SET token='wrong'",
        "UPDATE execution_canary_receipt_proofs SET tx_signature='wrong'",
        "UPDATE copy_signals SET token='wrong'",
        "UPDATE execution_canary_receipt_proofs SET slot='43'",
        "UPDATE execution_canary_receipt_proofs SET reason='not_complete'",
        "UPDATE orders SET status='execution_canary_confirmed_unreconciled'",
        "UPDATE execution_canary_receipt_proofs SET confirmation_status='processed'",
        "DELETE FROM execution_canary_receipt_proofs",
        "DELETE FROM execution_canary_receipt_facts",
        "PRAGMA foreign_keys=OFF; DELETE FROM copy_signals",
        "PRAGMA foreign_keys=OFF; DELETE FROM orders",
        "UPDATE fills SET cash_result_delta_lamports=0",
        "UPDATE fills SET wallet_native_delta_lamports=1,cash_result_delta_lamports=-2",
        "UPDATE fills SET qty_raw='2'",
        "UPDATE fills SET remaining_qty_raw='00'",
        "UPDATE execution_canary_receipt_facts SET wallet_native_post='1'",
        "UPDATE execution_canary_receipt_facts SET token_coverage='unresolved',token_coverage_reason='unknown'",
        "UPDATE execution_canary_receipt_facts SET transaction_fee='5'",
        "UPDATE execution_canary_receipt_facts SET block_time='9223372036854775807'",
        "UPDATE copy_signals SET side='buy'; UPDATE execution_canary_receipt_proofs SET side='buy'; UPDATE execution_canary_receipt_facts SET side='buy'",
        "PRAGMA ignore_check_constraints=ON; UPDATE fills SET accounting_basis='unknown_basis'",
    ] {
        let db=Db::new(1,3,0,1,0)?;
        first(&db,as_of()-Duration::seconds(1))?;
        let conn=db.conn()?;
        conn.execute_batch("PRAGMA foreign_keys=OFF; PRAGMA ignore_check_constraints=ON;")?;
        conn.execute_batch(sql).with_context(|| format!("install synthetic corruption: {sql}"))?;
        let before=snapshot(&db.conn()?)?;
        let result=db.store.execution_canary_sell_cash_day(as_of());
        assert!(result.is_err(),"accepted corruption: {sql}");
        assert_eq!(snapshot(&db.conn()?)?,before,"{sql}");
    }
    Ok(())
}

#[test]
fn malformed_future_cash_claim_is_not_legacy_or_an_empty_known_zero() -> Result<()> {
    let db = Db::new(1, 3, 0, 1, 0)?;
    first(&db, as_of() + Duration::days(10))?;
    sums(&db, as_of(), 0, 0, 0)?;
    db.conn()?
        .execute("UPDATE fills SET cash_result_delta_lamports=99", [])?;
    rejected_unchanged(&db)?;
    Ok(())
}

#[test]
fn missing_required_schema_errors_even_when_no_cash_event_is_selected() -> Result<()> {
    for table in [
        "fills",
        "orders",
        "copy_signals",
        "execution_canary_receipt_proofs",
        "execution_canary_receipt_facts",
    ] {
        let db = Db::new(1, 3, 0, 1, 0)?;
        db.conn()?
            .execute_batch(&format!("PRAGMA foreign_keys=OFF; DROP TABLE {table};"))?;
        rejected_unchanged(&db)?;
    }
    Ok(())
}

#[test]
fn duplicate_order_fills_after_index_corruption_cannot_double_count() -> Result<()> {
    let db = Db::new(1, 3, 0, 1, 0)?;
    first(&db, as_of() - Duration::seconds(1))?;
    db.conn()?.execute_batch(
        "DROP INDEX idx_fills_order_id;
        INSERT INTO fills(order_id,token,qty,qty_raw,qty_decimals,avg_price,fee,slippage_bps,
            accounting_basis,position_id,wallet_native_delta_lamports,entry_basis_lamports,
            cash_result_delta_lamports,accumulated_cash_result_lamports,remaining_qty_raw,
            remaining_cost_lamports,settlement_ts)
        SELECT order_id,token,qty,qty_raw,qty_decimals,avg_price,fee,slippage_bps,
            accounting_basis,position_id,wallet_native_delta_lamports,entry_basis_lamports,
            cash_result_delta_lamports,accumulated_cash_result_lamports,remaining_qty_raw,
            remaining_cost_lamports,settlement_ts FROM fills;",
    )?;
    rejected_unchanged(&db)?;
    Ok(())
}

#[test]
fn required_columns_are_checked_even_before_the_first_cash_fill() -> Result<()> {
    for (table, column) in [
        ("fills", "cash_result_delta_lamports"),
        ("execution_canary_receipt_facts", "wallet_native_pre"),
        ("execution_canary_receipt_proofs", "wallet_pubkey"),
        ("orders", "tx_signature"),
        ("copy_signals", "token"),
    ] {
        let db = Db::new(1, 3, 0, 1, 0)?;
        db.conn()?.execute_batch(&format!(
            "ALTER TABLE {table} RENAME COLUMN {column} TO unavailable_column;"
        ))?;
        rejected_unchanged(&db)?;
    }
    Ok(())
}
