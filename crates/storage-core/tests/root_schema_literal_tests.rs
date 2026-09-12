#[path = "common/source_sell_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::source_sell_handoff_schema;
use fixture::{snapshot, Db};

const SOL: &str = "So11111111111111111111111111111111111111112";

#[test]
fn root_healthy_no_capture_public_writer_creates_unknown_handoff() -> Result<()> {
    let db = Db::new()?;
    let event = db.sell("root-healthy-no-capture", "source-a");
    assert!(source_sell_handoff_schema::available(&db.conn()?)?);
    let written = db
        .store
        .insert_observed_swaps_batch_with_activity_days_measured(&[event.clone()])?;
    assert_eq!(written.inserted, vec![true]);
    let handoff = db
        .store
        .load_source_sell_handoff(&event.signature)?
        .expect("first modern no-capture SELL must retain Unknown");
    assert_eq!(handoff.disposition, "unknown");
    assert_eq!(handoff.original_position_id, None);
    assert_eq!(handoff.reason, "original_generation_unknown");
    assert_eq!(
        serde_json::to_value(&handoff.event)?,
        serde_json::to_value(&event)?
    );
    assert!(db.store.advance_source_sell_handoff()?.is_none());
    Ok(())
}

#[test]
fn root_changed_trigger_literal_is_rejected_before_public_writer_commit() -> Result<()> {
    let db = Db::new()?;
    let conn = db.conn()?;
    assert!(source_sell_handoff_schema::available(&conn)?);
    let original: String = conn.query_row(
        "SELECT sql FROM sqlite_master WHERE name='source_sell_handoff_after_observed'",
        [],
        |row| row.get(0),
    )?;
    let needle = format!("NEW.token_out='{SOL}'");
    assert_eq!(original.matches(&needle).count(), 1);
    // Explicit schema damage: one space INSIDE one literal changes its value.
    // No other schema object, migration marker or event field is modified.
    let damaged = original.replacen(&needle, &format!("NEW.token_out=' {SOL}'"), 1);
    assert_ne!(original, damaged);
    conn.execute_batch("DROP TRIGGER source_sell_handoff_after_observed")?;
    conn.execute_batch(&damaged)?;
    let event = db.sell("root-damaged-no-capture", "source-a");
    let before = snapshot(&conn, &[])?;
    let available = source_sell_handoff_schema::available(&conn);
    let result = db
        .store
        .insert_observed_swaps_batch_with_activity_days_measured(&[event])
        .map(|written| written.inserted);
    let observed: i64 = conn.query_row("SELECT COUNT(*) FROM observed_swaps", [], |r| r.get(0))?;
    let handoffs: i64 = conn.query_row("SELECT COUNT(*) FROM source_sell_handoffs", [], |r| {
        r.get(0)
    })?;
    eprintln!("ROOT_B57_LITERAL schema={available:?} writer={result:?} observed={observed} handoffs={handoffs}");
    assert!(
        result.is_err(),
        "corrupt recorded 0065 must refuse the public write: {result:?}"
    );
    assert!(
        available.is_err(),
        "literal changes are semantic schema damage"
    );
    assert_eq!(snapshot(&conn, &[])?, before);
    Ok(())
}
