#[path = "common/sell_settlement_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::*;
use SellSettlementUnsupported as U;

fn corrupt_fixture(db: &Db, sql: &str) -> Result<()> {
    // Only the synthetic writer bypasses constraints to exercise damaged/legacy data.
    let conn = db.conn()?;
    conn.pragma_update(None, "foreign_keys", false)?;
    conn.execute_batch(sql)?;
    Ok(())
}

#[test]
fn inventory_and_receipt_domains_are_explicitly_unsupported() -> Result<()> {
    for (sql,reason) in [
        ("DELETE FROM positions",U::NoOwnedPosition),
        ("UPDATE positions SET token='foreign'",U::NoOwnedPosition),
        ("UPDATE positions SET accounting_bucket='shadow'",U::NoOwnedPosition),
        ("UPDATE positions SET state='closed'",U::NoOwnedPosition),
        ("UPDATE positions SET qty_raw=NULL",U::MissingPositionQuantity),
        ("UPDATE positions SET qty_decimals=NULL",U::MissingPositionQuantity),
        ("UPDATE positions SET cost_lamports=NULL",U::MissingEntryBasis),
        ("UPDATE positions SET pnl_lamports=NULL",U::MissingAccumulatedCashResult),
        ("UPDATE positions SET qty_raw='0'",U::PositionQuantityOutOfRange),
        ("UPDATE positions SET qty_raw='18446744073709551616'",U::PositionQuantityOutOfRange),
        ("UPDATE positions SET qty_raw='340282366920938463463374607431768211456'",U::PositionQuantityOutOfRange),
        ("UPDATE positions SET qty_raw='2'",U::Oversell),
        ("UPDATE positions SET qty_decimals=9",U::DecimalsMismatch),
        ("UPDATE execution_canary_receipt_facts SET token_delta_raw='0'",U::NonNegativeTokenDelta),
        ("UPDATE execution_canary_receipt_facts SET token_delta_raw='3'",U::NonNegativeTokenDelta),
        ("UPDATE execution_canary_receipt_facts SET token_delta_raw='-18446744073709551616'",U::TokenQuantityOutOfRange),
        ("UPDATE execution_canary_receipt_facts SET token_delta_raw='-170141183460469231731687303715884105728'",U::TokenQuantityOutOfRange),
        ("UPDATE execution_canary_receipt_facts SET token_delta_raw=NULL,token_decimals=NULL,
          token_coverage='unresolved',token_coverage_reason='missing_token_balance'",U::UnresolvedTokenCoverage),
        ("DELETE FROM execution_canary_receipt_facts",U::MissingReceiptFacts),
        ("DELETE FROM orders",U::MissingOrder),
    ] {
        let db = Db::new(7,10,0,3,-9)?;
        corrupt_fixture(&db, sql)?;
        db.unsupported(reason).map_err(|e| e.context(sql))?;
    }
    Ok(())
}

#[test]
fn multiple_owned_positions_reject_but_foreign_or_closed_positions_are_ignored() -> Result<()> {
    let db = Db::new(7, 10, 0, 3, 0)?;
    let original = db.ready()?;
    // Synthetic legacy/damaged DB without the current uniqueness guard.
    db.conn()?
        .execute("DROP INDEX idx_positions_one_open_token_bucket", [])?;
    db.conn()?.execute_batch("INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,
        accounting_bucket,qty_raw,qty_decimals,cost_lamports,pnl_lamports)
        SELECT 'second',token,qty,cost_sol,opened_ts,state,accounting_bucket,qty_raw,qty_decimals,cost_lamports,pnl_lamports
        FROM positions WHERE position_id='owned'")?;
    db.unsupported(U::MultipleOwnedPositions)?;
    db.conn()?.execute(
        "UPDATE positions SET state='closed' WHERE position_id='second'",
        [],
    )?;
    assert_eq!(db.ready()?, original);
    db.conn()?.execute(
        "UPDATE positions SET state='open',accounting_bucket='shadow' WHERE position_id='second'",
        [],
    )?;
    assert_eq!(db.ready()?, original);
    Ok(())
}

#[test]
fn durable_identity_and_successful_proof_are_revalidated_on_every_call() -> Result<()> {
    use ReceiptFactsIdentityRejection as I;
    for (sql, reason) in [
        ("UPDATE orders SET tx_signature='other'", I::Identity),
        ("UPDATE orders SET tx_signature=NULL", I::Identity),
        ("UPDATE copy_signals SET token='other'", I::Identity),
        ("UPDATE copy_signals SET side='buy'", I::Identity),
        (
            "UPDATE execution_canary_receipt_proofs SET tx_signature='other'",
            I::Identity,
        ),
        (
            "UPDATE execution_canary_receipt_proofs SET wallet_pubkey='other'",
            I::Identity,
        ),
        (
            "UPDATE execution_canary_receipt_proofs SET token='other'",
            I::Identity,
        ),
        (
            "UPDATE execution_canary_receipt_proofs SET side='buy'",
            I::Identity,
        ),
        (
            "UPDATE execution_canary_receipt_proofs SET slot='43'",
            I::Slot,
        ),
        (
            "UPDATE execution_canary_receipt_proofs SET confirmation_status='failed'",
            I::Confirmation,
        ),
        (
            "UPDATE execution_canary_receipt_facts SET token='other'",
            I::Identity,
        ),
        (
            "UPDATE execution_canary_receipt_facts SET order_id='different'",
            I::Missing,
        ),
        ("DELETE FROM execution_canary_receipt_proofs", I::Missing),
        ("DELETE FROM copy_signals", I::Missing),
    ] {
        let db = Db::new(7, 10, 0, 3, 0)?;
        db.ready()?;
        corrupt_fixture(&db, sql)?;
        // Moving facts away leaves this order without facts, rather than a joined identity.
        let expected = if sql.contains("SET order_id=") {
            U::MissingReceiptFacts
        } else {
            U::DurableIdentity(reason)
        };
        db.unsupported(expected).map_err(|e| e.context(sql))?;
    }
    Ok(())
}

#[test]
fn valid_buy_is_unsupported_and_known_or_unknown_proof_slot_preserves_validation() -> Result<()> {
    let db = Db::new(7, 10, 0, 3, 0)?;
    db.conn()?.execute_batch(
        "UPDATE copy_signals SET side='buy';
        UPDATE execution_canary_receipt_proofs SET side='buy';
        UPDATE execution_canary_receipt_facts SET side='buy',token_delta_raw='3'",
    )?;
    db.unsupported(U::NotSell)?;
    let db = Db::new(7, 10, 0, 3, 0)?;
    let plan = db.ready()?;
    for status in ["confirmed", "finalized", "legacy_confirmed"] {
        db.conn()?.execute(
            "UPDATE execution_canary_receipt_proofs SET slot=NULL,confirmation_status=?1",
            [status],
        )?;
        assert_eq!(db.ready()?, plan);
    }
    db.conn()?.execute(
        "UPDATE execution_canary_receipt_facts SET token_coverage='proven_lifecycle'",
        [],
    )?;
    assert_eq!(
        db.ready()?.receipt.token_coverage,
        ReceiptTokenCoverage::ProvenLifecycle
    );
    Ok(())
}

#[test]
fn accounted_and_nonpending_orders_never_replan() -> Result<()> {
    for status in [
        EXECUTION_STATUS_CANARY_CONFIRMED,
        EXECUTION_STATUS_CANARY_SUBMITTED,
        EXECUTION_STATUS_CANARY_FAILED,
        EXECUTION_STATUS_CANARY_EXPIRED,
    ] {
        let db = Db::new(7, 10, 0, 3, 0)?;
        db.conn()?
            .execute("UPDATE orders SET status=?1", [status])?;
        db.unsupported(U::OrderNotPending)?;
    }
    for status in [
        EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
        EXECUTION_STATUS_CANARY_CONFIRMED,
    ] {
        let db = Db::new(7, 10, 0, 3, 0)?;
        db.conn()?
            .execute("UPDATE orders SET status=?1", [status])?;
        db.conn()?.execute(
            "INSERT INTO fills(order_id,token,qty,avg_price) VALUES(?1,'mint',3,123)",
            [ORDER],
        )?;
        db.unsupported(U::AlreadyAccounted)?;
    }
    Ok(())
}

#[test]
fn malformed_storage_values_are_errors_without_float_or_zero_fallback() -> Result<()> {
    for sql in [
        "UPDATE positions SET qty_raw='garbage'",
        "UPDATE positions SET qty_raw='07'",
        "UPDATE positions SET qty_raw='-1'",
        "UPDATE positions SET qty_raw=X'3132'",
        "UPDATE positions SET qty_decimals=1.5",
        "UPDATE positions SET qty_decimals=256",
        "UPDATE positions SET cost_lamports=-1",
        "UPDATE positions SET cost_lamports=1.5",
        "UPDATE positions SET cost_lamports='invalid'",
        "UPDATE positions SET cost_lamports=18446744073709551615",
        "UPDATE positions SET pnl_lamports=0.5",
        "UPDATE positions SET pnl_lamports='invalid'",
        "UPDATE positions SET pnl_lamports=9223372036854775808",
        "UPDATE positions SET pnl_lamports=-9223372036854775809",
        "UPDATE execution_canary_receipt_facts SET wallet_native_delta='1'",
        "UPDATE execution_canary_receipt_facts SET token_delta_raw='-03'",
        "UPDATE execution_canary_receipt_facts SET token_delta_raw='invalid'",
        "UPDATE execution_canary_receipt_facts SET wallet_native_pre='18446744073709551616'",
        "UPDATE execution_canary_receipt_proofs SET slot='invalid'",
    ] {
        let db = Db::new(7, 10, 0, 3, 0)?;
        corrupt_fixture(&db, sql)?;
        let before = snapshot(&db.conn()?)?;
        let error = db
            .store
            .plan_execution_canary_sell_settlement(ORDER)
            .expect_err(sql);
        assert!(error.downcast_ref::<U>().is_none(), "{sql}: {error:#}");
        assert!(
            error
                .downcast_ref::<ReceiptFactsIdentityRejection>()
                .is_none(),
            "{sql}: {error:#}"
        );
        assert_eq!(snapshot(&db.conn()?)?, before);
        // A failed read transaction must not poison subsequent calls.
        assert!(db
            .store
            .plan_execution_canary_sell_settlement(ORDER)
            .is_err());
    }
    Ok(())
}

#[test]
fn missing_schema_is_a_sql_error_and_never_created_by_planner() -> Result<()> {
    let db = Db::new(7, 10, 0, 3, 0)?;
    db.conn()?
        .execute("DROP TABLE execution_canary_receipt_facts", [])?;
    let before = snapshot(&db.conn()?)?;
    let ro = SqliteStore::open_read_only(&db.path)?;
    let error = ro.plan_execution_canary_sell_settlement(ORDER).unwrap_err();
    assert!(
        error.downcast_ref::<rusqlite::Error>().is_some(),
        "{error:#}"
    );
    assert_eq!(snapshot(&db.conn()?)?, before);
    Ok(())
}
