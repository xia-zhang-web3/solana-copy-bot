#[path = "common/source_sell_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::{ExecutionSellIntentOutcome, ExecutionSourceSellReject as Reject};
use fixture::*;

#[test]
fn two_sources_including_demoted_get_exact_generation_and_deterministic_witness() -> Result<()> {
    let mut db = Db::new()?;
    db.store
        .activate_follow_wallet("source-a", db.now - Duration::seconds(1), "fixture")?;
    let first = db.proven("a1", "source-a")?;
    db.proven("a2", "source-a")?;
    let second = db.proven("b", "source-b")?;
    db.store
        .deactivate_follow_wallet("source-a", db.now, "demoted")?;
    let position = db.position()?;
    for (sig, source, order) in [
        ("sell-a", "source-a", first),
        ("sell-b", "source-b", second),
    ] {
        let event = db.observed(sig, source)?;
        let before = snapshot(&db.conn()?, &[TABLE, "source_sell_signature_claims"])?;
        let row = inserted(
            db.store
                .stage_execution_source_sell_intent(&event, &position)?,
        );
        assert_eq!(row.position_id, position);
        assert_eq!(row.buy_witness.order_id, order);
        assert_eq!(
            row.buy_witness.signal_id,
            order.strip_prefix("exec-canary:").unwrap()
        );
        assert_eq!(row.buy_witness.source_wallet, source);
        assert_eq!(row.buy_execution_wallet, "execution-wallet");
        assert_eq!(row.buy_witness.tx_signature, format!("sig:{order}"));
        assert_eq!(format!("{:?}", row.event), format!("{event:?}"));
        let claim: (String, String) = db.conn()?.query_row(
            "SELECT owner,intent_id FROM source_sell_signature_claims WHERE signature=?1",
            [sig],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!(claim, ("legacy".into(), format!("source-sell:{sig}")));

        assert_eq!(
            snapshot(&db.conn()?, &[TABLE, "source_sell_signature_claims"])?,
            before
        );
        db.reopen()?;
        let read = db
            .store
            .load_execution_source_sell_intent(&row.intent_id)?
            .unwrap();
        assert_eq!(format!("{read:?}"), format!("{row:?}"));
        let before = snapshot(&db.conn()?, &[])?;
        let replay = existing(
            db.store
                .stage_execution_source_sell_intent(&event, &position)?,
        );
        assert_eq!(format!("{replay:?}"), format!("{row:?}"));
        assert_eq!(snapshot(&db.conn()?, &[])?, before);
    }
    for source in ["source-c", "execution-wallet"] {
        let event = db.observed(source, source)?;
        db.rejected(&event, &position, Reject::SourceNotProven)?;
    }
    assert!(db
        .store
        .load_execution_source_sell_intent("absent")?
        .is_none());
    Ok(())
}

#[test]
fn stale_generation_never_rebinds_with_equal_or_later_position_timestamps() -> Result<()> {
    for later in [false, true] {
        let mut db = Db::new()?;
        db.proven("a", "source-a")?;
        let a = db.position()?;
        let event = db.observed("staged-a", "source-a")?;
        let row = inserted(db.store.stage_execution_source_sell_intent(&event, &a)?);
        let stale = db.observed("not-yet-staged", "source-a")?;
        db.close()?;
        db.rejected(&event, &a, Reject::NoOwnedPosition)?;
        if later {
            db.now += Duration::seconds(2);
        }
        db.proven("b", "source-a")?;
        let b = db.position()?;
        assert_ne!(a, b);
        db.reopen()?;
        db.rejected(&stale, &a, Reject::GenerationMismatch)?;
        db.rejected(&event, &a, Reject::GenerationMismatch)?;
        db.rejected(&event, &b, Reject::GenerationMismatch)?;
        let read = db
            .store
            .load_execution_source_sell_intent(&row.intent_id)?
            .unwrap();
        assert_eq!(format!("{read:?}"), format!("{row:?}"));
        let next = db.observed("genuinely-new-b", "source-a")?;
        assert_eq!(
            inserted(db.store.stage_execution_source_sell_intent(&next, &b)?).position_id,
            b
        );
    }
    Ok(())
}

#[test]
fn replay_revalidates_witness_without_replacing_it_with_another_buy() -> Result<()> {
    let mut db = Db::new()?;
    let order = db.proven("a", "source-a")?;
    let position = db.position()?;
    let event = db.observed("sell", "source-a")?;
    let row = inserted(
        db.store
            .stage_execution_source_sell_intent(&event, &position)?,
    );
    db.proven("b", "source-a")?;
    let replay = existing(
        db.store
            .stage_execution_source_sell_intent(&event, &position)?,
    );
    assert_eq!(format!("{replay:?}"), format!("{row:?}"));
    // Test-only legacy downgrade of the original witness; second witness stays valid.
    db.conn()?.execute(
        "UPDATE fills SET position_id=NULL WHERE order_id=?1",
        [&order],
    )?;
    db.reopen()?;
    db.rejected(&event, &position, Reject::WitnessNoLongerProven)?;
    let next = db.observed("other-event", "source-a")?;
    assert_eq!(
        inserted(
            db.store
                .stage_execution_source_sell_intent(&next, &position)?
        )
        .buy_witness
        .order_id,
        "exec-canary:b"
    );
    Ok(())
}

#[test]
fn retained_staged_history_does_not_block_observed_retention_or_authorize_replay() -> Result<()> {
    let mut db = Db::new()?;
    db.proven("a", "source-a")?;
    let position = db.position()?;
    let event = db.observed("sell", "source-a")?;
    let row = inserted(
        db.store
            .stage_execution_source_sell_intent(&event, &position)?,
    );
    // Deliberate loss, bypassing retention's OPEN-source pin; preserve replay guards.
    db.conn()?.execute(
        "DELETE FROM observed_swaps WHERE signature=?1",
        [&event.signature],
    )?;
    db.reopen()?;
    assert_eq!(
        format!(
            "{:?}",
            db.store
                .load_execution_source_sell_intent(&row.intent_id)?
                .unwrap()
        ),
        format!("{row:?}")
    );
    db.rejected(&event, &position, Reject::ObservedEventMismatch)?;
    // Even if raw history is replaced with a different event under the same signature,
    // the original staged payload cannot be overwritten.
    let mut changed = event.clone();
    changed.amount_out += 0.1;
    assert!(
        !db.store.insert_observed_swap(&changed)?,
        "0065 tombstone rejects a mutated replay"
    );
    db.rejected(&changed, &position, Reject::ObservedEventMismatch)?;
    // Preserve the lower-level staging corruption check by explicitly bypassing the public writer.
    assert!(db.store.insert_observed_swap(&event)?);
    db.conn()?.execute(
        "UPDATE observed_swaps SET qty_out=?1 WHERE signature=?2",
        rusqlite::params![changed.amount_out, changed.signature],
    )?;
    db.rejected(&changed, &position, Reject::StagedEventConflict)?;
    Ok(())
}

#[test]
fn staged_records_never_enter_owned_queue_while_legacy_intent_still_does() -> Result<()> {
    let db = Db::new()?;
    db.proven("a", "source-a")?;
    let position = db.position()?;
    let staged = db.observed("staged", "source-a")?;
    let row = inserted(
        db.store
            .stage_execution_source_sell_intent(&staged, &position)?,
    );
    let statuses = [
        "",
        "shadow_recorded",
        "execution_sell_intent",
        TABLE,
        &row.intent_id,
        "' OR 1=1 --",
    ];
    for status in statuses {
        assert!(db
            .store
            .list_execution_quote_canary_owned_sell_signal_candidate_ids(status, db.now, 100)?
            .is_empty());
    }
    db.store
        .activate_follow_wallet("source-a", db.now, "legacy")?;
    let legacy = db.observed("real-legacy", "source-a")?;
    let ExecutionSellIntentOutcome::Inserted(signal) =
        db.store.record_execution_sell_intent(&legacy)?
    else {
        panic!("legacy intent")
    };
    for status in statuses {
        assert_eq!(
            db.store
                .list_execution_quote_canary_owned_sell_signal_candidate_ids(status, db.now, 100)?,
            vec![signal.signal_id.clone()]
        );
    }
    db.rejected(&legacy, &position, Reject::SignalAlreadyExists)?;
    // A real legacy signal created after staging also blocks a repeat.
    assert!(matches!(
        db.store.record_execution_sell_intent(&staged)?,
        ExecutionSellIntentOutcome::Inserted(_)
    ));
    db.rejected(&staged, &position, Reject::SignalAlreadyExists)?;
    Ok(())
}
