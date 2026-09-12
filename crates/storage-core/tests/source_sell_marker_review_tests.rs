#[path = "common/source_sell_promotion_fixture.rs"]
mod fixture;
use anyhow::Result;
use fixture::*;

fn actual_canonical_caller_after_moved_marker(move_marker: bool) -> Result<()> {
    let db = Db::new()?;
    db.proven("a", "source-a")?;
    let staged_a = prepare(&db, "exit-a", "source-a")?;
    let binding_a = promoted(
        db.store
            .promote_execution_source_sell_intent(&staged_a.intent_id)?,
    );
    let original_caller_a = signal(&db, &binding_a)?;
    if move_marker {
        // Corrupt only marker's forward key. Its original intent/staging survives.
        // The real canonical caller and its persisted signal are NOT rewritten.
        db.conn()?.execute(
            "UPDATE execution_source_sell_promotions SET signal_id='wrong-marker-key' WHERE signal_id=?1",
            [&binding_a.signal_id],
        )?;
    }
    db.close()?;
    db.proven("b", "source-a")?; // New actual generation, same fixture timestamp.
    let position_b = db.position()?;
    assert_ne!(staged_a.position_id, position_b);
    let candidates = db
        .store
        .list_execution_quote_canary_owned_sell_signal_candidate_ids(
            "shadow_recorded",
            db.now,
            10,
        )?;
    assert!(
        candidates.contains(&original_caller_a.signal_id),
        "actual A remains a candidate"
    );
    let before = snapshot(&db.conn()?, &[])?;
    let refused_promotion = db
        .store
        .promote_execution_source_sell_intent(&staged_a.intent_id);
    assert!(matches!(
        refused_promotion,
        Ok(Outcome::Rejected(_)) | Err(_)
    ));
    let actual_guard_a = db
        .store
        .execution_sell_intent_position_block_reason(&original_caller_a);
    assert_eq!(snapshot(&db.conn()?, &[])?, before);
    // New B is independently usable; the test does not require global rejection.
    let staged_b = prepare(&db, "exit-b", "source-a")?;
    let binding_b = promoted(
        db.store
            .promote_execution_source_sell_intent(&staged_b.intent_id)?,
    );
    let caller_b = signal(&db, &binding_b)?;
    assert_eq!(
        db.store
            .execution_sell_intent_position_block_reason(&caller_b)?,
        None
    );
    assert!(matches!(actual_guard_a, Ok(Some(_)) | Err(_)),
        "CANONICAL A LOST GENERATION GUARD: moved={move_marker}, A={}, B={position_b}, guard={actual_guard_a:?}", staged_a.position_id);
    Ok(())
}

#[test]
fn root_moved_marker_cannot_make_actual_canonical_a_legacy() -> Result<()> {
    actual_canonical_caller_after_moved_marker(true)
}
#[test]
fn root_intact_marker_rejects_old_a_and_allows_new_b_control() -> Result<()> {
    actual_canonical_caller_after_moved_marker(false)
}
