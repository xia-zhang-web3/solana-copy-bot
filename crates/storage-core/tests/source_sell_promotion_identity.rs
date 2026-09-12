#[path = "common/source_sell_promotion_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::ExecutionSellIntentOutcome;
use fixture::*;

#[test]
fn canonical_legacy_collision_is_never_adopted_and_legacy_guard_remains_positive() -> Result<()> {
    let db = Db::new()?;
    db.proven("a", "source-a")?;
    db.store
        .activate_follow_wallet("source-a", db.now, "test")?;
    let staged = prepare(&db, "exit", "source-a")?;
    let ExecutionSellIntentOutcome::Inserted(legacy) =
        db.store.record_execution_sell_intent(&staged.event)?
    else {
        panic!("legacy should pass")
    };
    rejected(&db, &staged.intent_id, Reject::SignalAlreadyExists)?;
    assert_eq!(
        db.store
            .execution_sell_intent_position_block_reason(&legacy)?,
        None
    );
    assert_eq!(
        db.conn()?.query_row(
            "SELECT count(*) FROM execution_source_sell_promotions",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    Ok(())
}

#[test]
fn own_replay_keeps_processed_status_order_and_retry_state_without_resurrection() -> Result<()> {
    let db = Db::new()?;
    db.proven("a", "source-a")?;
    let staged = prepare(&db, "exit", "source-a")?;
    let binding = promoted(
        db.store
            .promote_execution_source_sell_intent(&staged.intent_id)?,
    );
    let original = signal(&db, &binding)?;
    db.conn()?.execute("INSERT INTO execution_quote_canary_events(event_id,signal_id,wallet_id,token,side,quote_status,request_ts,signal_ts)
        VALUES('quote:preserve',?1,'source-a','mint','sell','error',?2,?2)",
        rusqlite::params![binding.signal_id, original.ts.to_rfc3339()])?;
    let order = db
        .store
        .reserve_execution_canary_order(&binding.signal_id, "tiny", db.now)?
        .order;
    db.store
        .mark_execution_canary_built(&order.order_id, db.now)?;
    db.conn()?.execute(
        "UPDATE orders SET attempt=7 WHERE order_id=?1",
        [&order.order_id],
    )?;
    db.conn()?.execute(
        "UPDATE copy_signals SET status='handled' WHERE signal_id=?1",
        [&binding.signal_id],
    )?;
    let current = signal(&db, &binding)?;
    let before = snapshot(&db.conn()?, &[])?;
    assert!(
        matches!(db.store.promote_execution_source_sell_intent(&staged.intent_id)?, Outcome::Existing(b) if b == binding)
    );
    assert_eq!(snapshot(&db.conn()?, &[])?, before);
    assert!(db
        .store
        .execution_sell_intent_position_block_reason(&original)?
        .is_some());
    assert_eq!(
        db.store
            .execution_sell_intent_position_block_reason(&current)?,
        Some("source_sell_signal_not_pending")
    );
    assert!(db
        .store
        .list_execution_quote_canary_owned_sell_signal_candidate_ids("shadow_recorded", db.now, 10)?
        .is_empty());
    Ok(())
}

#[test]
fn marked_signal_cannot_bypass_guard_by_changing_caller_fields() -> Result<()> {
    let db = Db::new()?;
    db.proven("a", "source-a")?;
    let staged = prepare(&db, "exit", "source-a")?;
    let original = signal(
        &db,
        &promoted(
            db.store
                .promote_execution_source_sell_intent(&staged.intent_id)?,
        ),
    )?;
    let before = snapshot(&db.conn()?, &[])?;
    for field in [
        "status", "side", "token", "wallet", "ts", "amount", "raw", "origin",
    ] {
        let mut caller = original.clone();
        match field {
            "status" => caller.status = "shadow_recorded".into(),
            "side" => caller.side = "buy".into(),
            "token" => caller.token = "other".into(),
            "wallet" => caller.wallet_id = "other".into(),
            "ts" => caller.ts += chrono::Duration::seconds(1),
            "amount" => caller.notional_sol += 1.0,
            "raw" => caller.notional_lamports = None,
            _ => caller.notional_origin = "approximate".into(),
        }
        assert_eq!(
            db.store
                .execution_sell_intent_position_block_reason(&caller)?,
            Some("source_sell_signal_conflict"),
            "{field}"
        );
    }
    assert_eq!(snapshot(&db.conn()?, &[])?, before);
    Ok(())
}

#[test]
fn missing_or_malformed_associations_refuse_without_legacy_fallback_or_partial_repair() -> Result<()>
{
    for case in [
        "signal",
        "staging",
        "mapping",
        "wrong-stage",
        "wrong-signal",
        "bad-time",
        "signal-data",
        "duplicate-intent",
    ] {
        let db = Db::new()?;
        db.proven("a", "source-a")?;
        let a = prepare(&db, "exit-a", "source-a")?;
        let b = prepare(&db, "exit-b", "source-a")?;
        let binding = promoted(
            db.store
                .promote_execution_source_sell_intent(&a.intent_id)?,
        );
        let mut caller = signal(&db, &binding)?;
        match case {
            "signal" => {
                db.conn()?.execute(
                    "DELETE FROM copy_signals WHERE signal_id=?1",
                    [&binding.signal_id],
                )?;
                caller.status = "anything".into();
            }
            "staging" => {
                db.conn()?.execute(
                    "DELETE FROM execution_source_sell_intents WHERE intent_id=?1",
                    [&a.intent_id],
                )?;
            }
            "mapping" => {
                db.conn()?
                    .execute("DELETE FROM execution_source_sell_promotions", [])?;
            }
            "wrong-stage" => {
                db.conn()?.execute(
                    "UPDATE execution_source_sell_promotions SET intent_id=?1",
                    [&b.intent_id],
                )?;
            }
            "wrong-signal" => {
                db.conn()?.execute(
                    "UPDATE execution_source_sell_promotions SET signal_id='wrong'",
                    [],
                )?;
                caller.signal_id = "wrong".into();
            }
            "bad-time" => {
                db.conn()?.execute(
                    "UPDATE execution_source_sell_promotions SET promoted_at='bad'",
                    [],
                )?;
            }
            "duplicate-intent" => {
                db.conn()?.execute_batch("ALTER TABLE execution_source_sell_promotions RENAME TO corrupt_original;
                    CREATE TABLE execution_source_sell_promotions AS SELECT * FROM corrupt_original;
                    INSERT INTO execution_source_sell_promotions SELECT 'extra-signal',intent_id,promoted_at FROM corrupt_original;
                    DROP TABLE corrupt_original;")?;
            }
            _ => {
                db.conn()?.execute(
                    "UPDATE copy_signals SET side='buy' WHERE signal_id=?1",
                    [&binding.signal_id],
                )?;
            }
        }
        let before = snapshot(&db.conn()?, &[])?;
        let outcome = db.store.promote_execution_source_sell_intent(&a.intent_id);
        assert!(
            matches!(outcome, Ok(Outcome::Rejected(_)) | Err(_)),
            "{case}: {outcome:?}"
        );
        let guard = db
            .store
            .execution_sell_intent_position_block_reason(&caller);
        if case == "mapping" {
            assert_eq!(guard?, None, "no marker retains legacy semantics");
        } else {
            assert!(matches!(guard, Ok(Some(_)) | Err(_)), "{case}: {guard:?}");
        }
        assert_eq!(snapshot(&db.conn()?, &[])?, before);
    }
    Ok(())
}
