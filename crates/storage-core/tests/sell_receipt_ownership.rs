#[path = "common/sell_cash_day_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::*;
use fixture::*;

fn money(db: &Db) -> Result<(String, i64, i64, i64)> {
    Ok(db.conn()?.query_row(
        "SELECT qty_raw,cost_lamports,pnl_lamports,(SELECT COUNT(*) FROM fills)
         FROM positions WHERE position_id='owned'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?)
}

fn refused(db: &Db, facts: &ExecutionCanaryReceiptFacts) -> Result<()> {
    let before = snapshot(&db.conn()?)?;
    let money_before = money(db)?;
    let result = db
        .store
        .apply_execution_canary_sell_settlement(facts, as_of());
    let money_after = money(db)?;
    println!("SELL_OWNERSHIP before={money_before:?} after={money_after:?} result={result:?}");
    let error = result.expect_err("a second receipt owner must not account inventory/cash");
    let reason = error
        .downcast_ref::<SellSettlementUnsupported>()
        .expect("ownership refusal must preserve the app typed-pending boundary");
    assert_eq!(format!("{reason:?}"), "ReceiptAlreadyClaimed");
    assert_eq!(money_after, money_before);
    assert_eq!(snapshot(&db.conn()?)?, before);
    assert!(db
        .store
        .load_execution_canary_cash_settlement(&facts.order_id)?
        .is_none());
    assert!(!db.store.execution_canary_fill_exists(&facts.order_id)?);
    assert!(db.store.execution_canary_accounting_pending()?);
    assert!(db
        .store
        .execution_canary_token_accounting_pending(&facts.token)?);
    Ok(())
}

#[test]
fn actual_second_sell_cannot_merge_cash_or_close_with_the_same_receipt() -> Result<()> {
    for old_raw in [3, 2] {
        for native in [-5, 0, 11] {
            let mut db = Db::new(old_raw, (old_raw * 3) as i64, 0, 1, native)?;
            first(&db, as_of() - Duration::days(1))?;
            assert_eq!(
                money(&db)?,
                (
                    (old_raw - 1).to_string(),
                    ((old_raw - 1) * 3) as i64,
                    native as i64 - 3,
                    1
                )
            );
            let mut second = db.facts(1, native);
            second.order_id = "exec-canary:duplicate".into();
            let second = prepare_identity(&db, second, "tiny")?;
            db.store = SqliteStore::open(&db.path)?;
            refused(&db, &second)?;
        }
    }
    Ok(())
}

#[test]
fn closed_reopen_new_generation_does_not_free_the_old_receipt() -> Result<()> {
    let mut db = Db::new(1, 3, 0, 1, 0)?;
    let initial = first(&db, as_of())?;
    assert_eq!(initial.remaining_quantity.raw(), 0);
    db.store = SqliteStore::open(&db.path)?;
    // Explicit imported inventory for the next generation, as in reader fixtures.
    db.conn()?.execute_batch(
        "INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,
        accounting_bucket,qty_raw,qty_decimals,cost_lamports,pnl_lamports)
        VALUES('new-generation','mint',0.003,0.000000009,'2026-09-07T00:00:00Z','open',
        'execution_canary','3',3,9,0)",
    )?;
    let mut second = db.facts(1, 0);
    second.order_id = "exec-canary:next-generation".into();
    let second = prepare_identity(&db, second, "tiny")?;
    refused(&db, &second)?;
    let before = snapshot(&db.conn()?)?;
    let replay = db
        .store
        .apply_execution_canary_sell_settlement(&db.facts(1, 0), as_of())?;
    assert!(replay.already_accounted);
    assert_eq!(replay.settlement, initial);
    assert_eq!(snapshot(&db.conn()?)?, before);
    Ok(())
}

#[test]
fn distinct_receipts_and_wallets_account_and_replay_after_full_close() -> Result<()> {
    for other_wallet in [false, true] {
        let mut db = Db::new(2, 6, 0, 1, -5)?;
        let initial = first(&db, as_of())?;
        let mut second = db.facts(1, 11);
        second.order_id = "exec-canary:healthy".into();
        if other_wallet {
            second.wallet_pubkey = "other-wallet".into();
        } else {
            second.tx_signature = "other-receipt".into();
        }
        let second = prepare_identity(&db, second, "tiny")?;
        let out = db
            .store
            .apply_execution_canary_sell_settlement(&second, as_of())?;
        assert!(!out.already_accounted);
        assert_eq!(out.settlement.remaining_quantity.raw(), 0);
        assert_eq!(money(&db)?, ("0".into(), 0, 0, 2)); // (-5-3) + (11-3)
        assert!(!db.store.execution_canary_accounting_pending()?);
        db.store = SqliteStore::open(&db.path)?;
        let before = snapshot(&db.conn()?)?;
        for (facts, expected) in [(db.facts(1, -5), initial), (second, out.settlement)] {
            let replay = db
                .store
                .apply_execution_canary_sell_settlement(&facts, as_of() + Duration::days(1))?;
            assert!(replay.already_accounted);
            assert_eq!(replay.settlement, expected);
        }
        assert_eq!(snapshot(&db.conn()?)?, before);
    }
    Ok(())
}

#[test]
fn typed_refusal_keeps_a_pending_while_independent_b_can_settle() -> Result<()> {
    let db = Db::new(3, 9, 0, 1, 0)?;
    let mut counterpart = db.facts(1, 0);
    counterpart.order_id = "exec-canary:other-claim".into();
    counterpart.token = "other-mint".into();
    prepare_identity(&db, counterpart, "other-route")?;
    let pending_proof = db.store.load_execution_canary_receipt_proof(ORDER)?;
    refused(&db, &db.facts(1, 0))?;
    let healthy = prepare(&db, "exec-canary:healthy-b", 1, 9)?;
    let result = db
        .store
        .apply_execution_canary_sell_settlement(&healthy, as_of())?;
    assert!(!result.already_accounted);
    assert_eq!(money(&db)?, ("2".into(), 6, 6, 1));
    assert_eq!(
        db.store.load_execution_canary_receipt_proof(ORDER)?,
        pending_proof
    );
    assert!(!db.store.execution_canary_fill_exists(ORDER)?);
    assert!(db.store.execution_canary_accounting_pending()?);
    assert!(db.store.execution_canary_token_accounting_pending("mint")?);
    Ok(())
}
