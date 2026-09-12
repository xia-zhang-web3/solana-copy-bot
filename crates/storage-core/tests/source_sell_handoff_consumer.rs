#[path = "common/source_sell_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::{
    ExecutionSourceSellOutcome as Outcome, SourceSellCandidate as Candidate,
};
use fixture::*;

#[test]
fn stage_disposition_and_commit_are_one_transaction_and_retry_is_idempotent() -> Result<()> {
    for fault in ["abort", "ignore", "commit"] {
        let mut db = Db::new()?;
        db.proven("buy", "source-a")?;
        let e = db.sell("exit", "source-a");
        let p = db.position()?;
        db.store
            .insert_observed_swaps_with_candidates(&[e.clone()], &[Some(Candidate::new(&e, &p))])?;
        let sql=match fault {
            "abort"=>"CREATE TRIGGER fault BEFORE UPDATE ON source_sell_handoffs BEGIN SELECT RAISE(ABORT,'disposition fault'); END;",
            "ignore"=>"CREATE TRIGGER fault BEFORE UPDATE ON source_sell_handoffs BEGIN SELECT RAISE(IGNORE); END;",
            _=>"CREATE TABLE parent(id PRIMARY KEY); CREATE TABLE effect(id REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fault AFTER UPDATE ON source_sell_handoffs BEGIN INSERT INTO effect VALUES(99); END;",
        };
        db.conn()?.execute_batch(sql)?;
        let before = snapshot(&db.conn()?, &[])?;
        assert!(
            db.store.process_source_sell_handoff(&e.signature).is_err(),
            "{fault}"
        );
        assert_eq!(snapshot(&db.conn()?, &[])?, before, "{fault}");
        db.conn()?.execute_batch("DROP TRIGGER fault")?;
        let first = inserted(db.store.process_source_sell_handoff(&e.signature)?);
        db.reopen()?; // Committed result is discarded by the caller before reopen.
        let before = snapshot(&db.conn()?, &[])?;
        let again = existing(db.store.process_source_sell_handoff(&e.signature)?);
        assert_eq!(format!("{first:?}"), format!("{again:?}"));
        assert_eq!(snapshot(&db.conn()?, &[])?, before);
        db.store
            .promote_execution_source_sell_intent(&first.intent_id)?;
        let promoted = snapshot(&db.conn()?, &[])?;
        assert!(matches!(
            db.store.process_source_sell_handoff(&e.signature)?,
            Outcome::Existing(_)
        ));
        assert_eq!(snapshot(&db.conn()?, &[])?, promoted);
    }
    Ok(())
}

#[test]
fn cursor_progress_survives_retry_restart_arrivals_and_wrap() -> Result<()> {
    let mut db = Db::new()?;
    db.proven("buy", "source-a")?;
    let b = db.sell("older-b", "source-a");
    let a = db.sell("newer-a", "source-a");
    let p = db.position()?;
    for e in [&b, &a] {
        db.store
            .insert_observed_swaps_with_candidates(&[e.clone()], &[Some(Candidate::new(e, &p))])?;
    }
    assert_eq!(
        db.store
            .advance_source_sell_handoff()?
            .unwrap()
            .event
            .signature,
        a.signature
    );
    db.conn()?.execute_batch("CREATE TRIGGER fault BEFORE INSERT ON execution_source_sell_intents WHEN NEW.event_signature='newer-a' BEGIN SELECT RAISE(ABORT,'database is locked'); END;")?;
    assert!(db.store.process_source_sell_handoff(&a.signature).is_err());
    db.reopen()?;
    let c = db.sell("arrival-c", "source-a");
    db.store
        .insert_observed_swaps_with_candidates(&[c.clone()], &[Some(Candidate::new(&c, &p))])?;
    assert_eq!(
        db.store
            .advance_source_sell_handoff()?
            .unwrap()
            .event
            .signature,
        b.signature
    );
    inserted(db.store.process_source_sell_handoff(&b.signature)?);
    assert!(db.store.advance_source_sell_handoff()?.is_none());
    db.conn()?.execute_batch("DROP TRIGGER fault")?;
    assert_eq!(
        db.store
            .advance_source_sell_handoff()?
            .unwrap()
            .event
            .signature,
        c.signature
    );
    inserted(db.store.process_source_sell_handoff(&c.signature)?);
    db.reopen()?;
    assert_eq!(
        db.store
            .advance_source_sell_handoff()?
            .unwrap()
            .event
            .signature,
        a.signature
    );
    inserted(db.store.process_source_sell_handoff(&a.signature)?);
    assert!(db.store.advance_source_sell_handoff()?.is_none());
    Ok(())
}

#[test]
fn pending_selection_is_indexed_and_bounded() -> Result<()> {
    let db = Db::new()?;
    db.proven("buy", "source-a")?;
    let p = db.position()?;
    for i in 0..70 {
        let e = db.sell(&format!("row-{i}"), "source-a");
        db.store
            .insert_observed_swaps_with_candidates(&[e.clone()], &[Some(Candidate::new(&e, &p))])?;
    }
    let conn = db.conn()?;
    let sql="SELECT sequence FROM source_sell_handoffs WHERE disposition='pending' AND sequence<?1 ORDER BY sequence DESC LIMIT 1";
    let plans = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
        .query_map([51], |r| r.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert!(
        plans
            .iter()
            .any(|p| p.contains("SEARCH") && p.contains("idx_source_sell_handoff_pending")),
        "{plans:?}"
    );
    assert!(
        plans
            .iter()
            .all(|p| !p.contains("SCAN") && !p.contains("TEMP B-TREE")),
        "{plans:?}"
    );
    let mut stmt = conn.prepare(sql)?;
    let selected: i64 = stmt.query_row([51], |r| r.get(0))?;
    assert_eq!(selected, 50);
    let visits = stmt.get_status(rusqlite::StatementStatus::VmStep);
    assert!(visits < 40, "bounded seek: {visits}");
    eprintln!("B57_QUERY_PLAN {plans:?}; VM steps={visits}");
    Ok(())
}

#[test]
fn continuous_new_arrivals_cannot_starve_older_pending_during_restart() -> Result<()> {
    let mut db = Db::new()?;
    db.proven("buy", "source-a")?;
    let p = db.position()?;
    for i in 0..5 {
        let e = db.sell(&format!("old-{i}"), "source-a");
        db.store
            .insert_observed_swaps_with_candidates(&[e.clone()], &[Some(Candidate::new(&e, &p))])?;
    }
    assert_eq!(
        db.store
            .advance_source_sell_handoff()?
            .unwrap()
            .event
            .signature,
        "old-4"
    );
    // Leave selected A pending on a transient failure; restart after every further visit.
    db.conn()?.execute_batch("CREATE TRIGGER fault BEFORE INSERT ON execution_source_sell_intents WHEN NEW.event_signature='old-4' BEGIN SELECT RAISE(ABORT,'database is locked'); END;")?;
    assert!(db.store.process_source_sell_handoff("old-4").is_err());
    for i in (0..4).rev() {
        db.reopen()?;
        let e = db.sell(&format!("arrival-{i}"), "source-a");
        db.store
            .insert_observed_swaps_with_candidates(&[e.clone()], &[Some(Candidate::new(&e, &p))])?;
        let selected = db.store.advance_source_sell_handoff()?.unwrap();
        assert_eq!(selected.event.signature, format!("old-{i}"));
        inserted(
            db.store
                .process_source_sell_handoff(&selected.event.signature)?,
        );
    }
    assert!(db.store.advance_source_sell_handoff()?.is_none());
    db.conn()?.execute_batch("DROP TRIGGER fault")?;
    for i in 0..4 {
        let selected = db.store.advance_source_sell_handoff()?.unwrap();
        assert_eq!(selected.event.signature, format!("arrival-{i}"));
        inserted(
            db.store
                .process_source_sell_handoff(&selected.event.signature)?,
        );
    }
    assert_eq!(
        db.store
            .advance_source_sell_handoff()?
            .unwrap()
            .event
            .signature,
        "old-4"
    );
    inserted(db.store.process_source_sell_handoff("old-4")?);
    Ok(())
}
