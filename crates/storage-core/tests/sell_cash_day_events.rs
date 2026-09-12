#[path = "common/sell_cash_day_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use fixture::*;

#[test]
fn partial_loss_profit_and_final_close_keep_per_event_net_and_gross() -> Result<()> {
    let db = Db::new(3, 9, 0, 1, -4)?;
    let at = as_of() - Duration::seconds(10);
    let a = first(&db, at)?;
    assert_eq!(a.cash_result_delta.as_i128(), -7);
    assert!(db
        .store
        .load_execution_canary_open_position("mint")?
        .is_some());
    let v = sums(&db, at + Duration::nanoseconds(1), 1, -7, 7)?;
    assert_eq!(
        (v.known_events.partial_events, v.known_events.full_events),
        (1, 0)
    );
    let b = next(&db, "exec-canary:profit", 1, 6, at + Duration::seconds(1))?;
    assert_eq!(b.cash_result_delta.as_i128(), 3);
    assert!(db
        .store
        .load_execution_canary_open_position("mint")?
        .is_some());
    sums(&db, at + Duration::seconds(2), 2, -4, 7)?;
    let c = next(&db, "exec-canary:final", 1, 1, at + Duration::seconds(2))?;
    assert_eq!(c.cash_result_delta.as_i128(), -2);
    assert!(db
        .store
        .load_execution_canary_open_position("mint")?
        .is_none());
    assert_eq!(
        a.allocated_entry_basis.as_u64()
            + b.allocated_entry_basis.as_u64()
            + c.allocated_entry_basis.as_u64(),
        9
    );
    let final_view = sums(&db, as_of(), 3, -6, 9)?;
    assert_eq!(
        (
            final_view.known_events.negative_events,
            final_view.known_events.positive_events,
            final_view.known_events.zero_events
        ),
        (2, 1, 0)
    );
    assert_eq!(
        (
            final_view.known_events.partial_events,
            final_view.known_events.full_events
        ),
        (2, 1)
    );
    // Current CLOSED state cannot relocate an older partial or change its classification.
    assert_eq!(view(&db, at + Duration::nanoseconds(1))?, v);
    for _ in 0..2 {
        db.conn()?.execute("INSERT INTO shadow_closed_trades(signal_id,wallet_id,token,qty,entry_cost_sol,exit_value_sol,pnl_sol,opened_ts,closed_ts)
            VALUES('sell-signal','leader','mint',1,999,999,0,?1,?1)",[db.now.to_rfc3339()])?;
    }
    assert_eq!(view(&db, as_of())?, final_view);
    Ok(())
}

#[test]
fn signed_native_zero_and_allocated_basis_are_not_extra_fee_expenses() -> Result<()> {
    for (native, expected) in [(-13, -36), (0, -23), (23, 0), (29, 6)] {
        let db = Db::new(7, 23, 0, 7, native)?;
        // Fee facts are evidence already reflected in cash; do not subtract again.
        let mut facts = db.facts(7, native);
        facts.transaction_fee = Some(copybot_core_types::Lamports::new(5));
        facts.fee_coverage = copybot_storage_core::ReceiptFeeCoverage::Known;
        facts.fee_payer = Some("wallet".into());
        db.store
            .record_execution_canary_receipt_facts(&facts, db.now)?;
        db.store
            .apply_execution_canary_sell_settlement(&facts, as_of() - Duration::seconds(1))?;
        let v = sums(
            &db,
            as_of(),
            1,
            expected,
            if expected < 0 {
                expected.unsigned_abs()
            } else {
                0
            },
        )?;
        assert_eq!(v.known_events.zero_events, u64::from(expected == 0));
        assert_eq!(v.known_events.full_events, 1);
    }
    Ok(())
}

#[test]
fn odd_basis_conserves_across_several_exits() -> Result<()> {
    let db = Db::new(7, 23, 0, 2, 5)?;
    let at = as_of() - Duration::seconds(1);
    let a = first(&db, at)?;
    let b = next(&db, "exec-canary:second", 4, -3, at)?;
    let c = next(&db, "exec-canary:last", 1, 0, at)?;
    assert_eq!(
        (
            a.allocated_entry_basis.as_u64(),
            b.allocated_entry_basis.as_u64(),
            c.allocated_entry_basis.as_u64()
        ),
        (7, 13, 3)
    );
    assert_eq!(c.remaining_entry_basis.as_u64(), 0);
    assert_eq!(c.remaining_quantity.raw(), 0);
    sums(&db, as_of(), 3, -21, 21)?;
    Ok(())
}

#[test]
fn checked_totals_exceed_i64_and_json_precision_without_min_abs_overflow() -> Result<()> {
    let db = Db::new(1, 0, 0, 1, i128::from(i64::MIN))?;
    let at = as_of() - Duration::seconds(1);
    first(&db, at)?;
    sums(&db, as_of(), 1, i128::from(i64::MIN), 1_u128 << 63)?;
    for (i, native) in [i64::MIN, i64::MAX, i64::MAX].into_iter().enumerate() {
        // New imported exact inventory; every resulting fill is still made by the writer.
        db.conn()?.execute("INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,accounting_bucket,qty_raw,qty_decimals,cost_lamports,pnl_lamports)
            VALUES(?1,'mint',0.001,0,?2,'open',?3,'1',3,0,0)",rusqlite::params![format!("inventory-{i}"),db.now.to_rfc3339(),copybot_storage_core::EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET])?;
        next(
            &db,
            &format!("exec-canary:wide-{i}"),
            1,
            i128::from(native),
            at,
        )?;
        if i == 0 {
            sums(&db, as_of(), 2, -(1_i128 << 64), 1_u128 << 64)?;
        }
    }
    let v = sums(&db, as_of(), 4, -2, 1_u128 << 64)?;
    assert_eq!(
        (
            v.known_events.negative_events,
            v.known_events.positive_events
        ),
        (2, 2)
    );
    Ok(())
}

#[test]
fn recovery_orphan_position_is_a_proven_event_here() -> Result<()> {
    let db = Db::new(1, 3, 0, 1, 0)?;
    db.conn()?.execute(
        "UPDATE positions SET position_id='exec-canary-pos:recovery-orphan:cash-day'",
        [],
    )?;
    first(&db, as_of() - Duration::seconds(1))?;
    sums(&db, as_of(), 1, -3, 3)?;
    // The existing cap's exclusion remains intact; this API is not its replacement yet.
    assert_eq!(
        db.store
            .execution_canary_entry_cost(as_of())?
            .closed_loss
            .positions,
        0
    );
    Ok(())
}

#[test]
fn events_cover_distinct_wallets_routes_and_mints_without_config_filter() -> Result<()> {
    use copybot_storage_core::EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET;
    let db = Db::new(1, 3, 0, 1, 0)?;
    let at = as_of() - Duration::seconds(1);
    first(&db, at)?;
    db.conn()?.execute("INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,accounting_bucket,qty_raw,qty_decimals,cost_lamports,pnl_lamports)
        VALUES('other-position','other-mint',0.001,0.000000007,?1,'open',?2,'1',3,7,0)",rusqlite::params![db.now.to_rfc3339(),EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET])?;
    let mut fresh = db.facts(1, 10);
    fresh.order_id = "exec-canary:other-wallet".into();
    fresh.tx_signature = "other-signature".into();
    fresh.wallet_pubkey = "other-wallet".into();
    fresh.token = "other-mint".into();
    let fresh = prepare_identity(&db, fresh, "another-route")?;
    db.store
        .apply_execution_canary_sell_settlement(&fresh, at)?;
    sums(&db, as_of(), 2, 0, 3)?;
    Ok(())
}
