#[path = "common/source_sell_promotion_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::ExecutionSellIntentOutcome;
use fixture::*;

fn move_marker(db: &Db) -> Result<()> {
    db.conn()?.execute(
        "UPDATE execution_source_sell_promotions SET signal_id='wrong-marker-key'",
        [],
    )?;
    Ok(())
}

#[test]
fn moved_marker_lookup_uses_durable_fields_and_preserves_separator_identities() -> Result<()> {
    let mut db = Db::new()?;
    let source = "source:a:with:separators";
    db.proven("a", source)?;
    let staged = prepare(&db, "exit:a:with:separators", source)?;
    let binding = promoted(
        db.store
            .promote_execution_source_sell_intent(&staged.intent_id)?,
    );
    let original = signal(&db, &binding)?;
    move_marker(&db)?;
    db.close()?;
    db.proven("b", source)?;
    db.reopen()?;
    let before = snapshot(&db.conn()?, &[])?;
    for field in [
        "original", "status", "side", "token", "wallet", "ts", "amount", "raw", "origin",
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
            "origin" => caller.notional_origin = "approximate".into(),
            _ => {}
        }
        assert_eq!(caller.signal_id, original.signal_id);
        assert_eq!(
            db.store
                .execution_sell_intent_position_block_reason(&caller)?,
            Some("source_sell_binding_conflict"),
            "{field}"
        );
    }
    assert_eq!(snapshot(&db.conn()?, &[])?, before);
    Ok(())
}

#[test]
fn partial_or_malformed_reverse_association_refuses_without_writes_after_reopen() -> Result<()> {
    for case in [
        "signal",
        "signal-token",
        "signal-wallet",
        "staging",
        "bad-stage-time",
        "canonical",
        "bad-marker-time",
        "duplicate",
        "sql",
    ] {
        let mut db = Db::new()?;
        db.proven("a", "source-a")?;
        let staged = prepare(&db, "exit-a", "source-a")?;
        let binding = promoted(
            db.store
                .promote_execution_source_sell_intent(&staged.intent_id)?,
        );
        let caller = signal(&db, &binding)?;
        move_marker(&db)?;
        db.close()?;
        db.proven("b", "source-a")?;
        match case {
            "signal" => {
                db.conn()?.execute(
                    "DELETE FROM copy_signals WHERE signal_id=?1",
                    [&caller.signal_id],
                )?;
            }
            "signal-token" => {
                db.conn()?.execute(
                    "UPDATE copy_signals SET token='other' WHERE signal_id=?1",
                    [&caller.signal_id],
                )?;
            }
            "signal-wallet" => {
                db.conn()?.execute(
                    "UPDATE copy_signals SET wallet_id='other' WHERE signal_id=?1",
                    [&caller.signal_id],
                )?;
            }
            "staging" => {
                db.conn()?.execute(
                    "DELETE FROM execution_source_sell_intents WHERE intent_id=?1",
                    [&staged.intent_id],
                )?;
            }
            "bad-stage-time" => {
                db.conn()?.execute(
                    "UPDATE execution_source_sell_intents SET staged_at='bad'",
                    [],
                )?;
            }
            "canonical" => {
                db.conn()?.execute(
                    "UPDATE execution_source_sell_intents SET source_wallet='other-source'",
                    [],
                )?;
            }
            "bad-marker-time" => {
                db.conn()?.execute(
                    "UPDATE execution_source_sell_promotions SET promoted_at='bad'",
                    [],
                )?;
            }
            "duplicate" => {
                db.conn()?.execute_batch("ALTER TABLE execution_source_sell_promotions RENAME TO corrupt_original;
                    CREATE TABLE execution_source_sell_promotions AS SELECT * FROM corrupt_original;
                    INSERT INTO execution_source_sell_promotions SELECT 'another-key',intent_id,promoted_at FROM corrupt_original;
                    DROP TABLE corrupt_original;")?;
            }
            _ => {
                db.conn()?.execute_batch("ALTER TABLE execution_source_sell_promotions RENAME COLUMN intent_id TO broken_intent;")?;
            }
        }
        db.reopen()?;
        let before = snapshot(&db.conn()?, &[])?;
        let guard = db
            .store
            .execution_sell_intent_position_block_reason(&caller);
        assert!(guard.is_err(), "{case}: {guard:?}");
        assert_eq!(snapshot(&db.conn()?, &[])?, before, "{case}");
    }
    Ok(())
}

#[test]
fn legacy_with_or_without_staging_is_not_captured_by_another_reverse_marker() -> Result<()> {
    for has_staging in [false, true] {
        let db = Db::new()?;
        db.proven("a", "source-a")?;
        // Same wallet/mint/time and signature prefix: none is authority for this event.
        let other = prepare(&db, "exit-prefix", "source-a")?;
        promoted(
            db.store
                .promote_execution_source_sell_intent(&other.intent_id)?,
        );
        move_marker(&db)?;
        db.store
            .activate_follow_wallet("source-a", db.now, "test")?;
        let event = db.observed("exit-prefix-extra", "source-a")?;
        let staged = if has_staging {
            Some(inserted(db.store.stage_execution_source_sell_intent(
                &event,
                &db.position()?,
            )?))
        } else {
            None
        };
        let ExecutionSellIntentOutcome::Inserted(legacy) =
            db.store.record_execution_sell_intent(&event)?
        else {
            panic!("legacy should pass")
        };
        let before = snapshot(&db.conn()?, &[])?;
        assert_eq!(
            db.store
                .execution_sell_intent_position_block_reason(&legacy)?,
            None
        );
        if let Some(staged) = staged {
            rejected(&db, &staged.intent_id, Reject::SignalAlreadyExists)?;
        }
        assert_eq!(snapshot(&db.conn()?, &[])?, before);
        // The shared guard also sees unrelated BUY signals; their canonical prefix
        // alone must not route them through the source SELL proof requirements.
        let mut buy = legacy.clone();
        buy.side = "buy".into();
        buy.signal_id = "shadow:buy-event:source-a:buy:mint".into();
        buy.status = "shadow_recorded".into();
        assert!(db.store.insert_copy_signal(&buy)?);
        let before = snapshot(&db.conn()?, &[])?;
        assert_eq!(
            db.store.execution_sell_intent_position_block_reason(&buy)?,
            None
        );
        assert_eq!(snapshot(&db.conn()?, &[])?, before);
    }
    Ok(())
}
