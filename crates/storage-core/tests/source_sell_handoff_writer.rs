#[path = "common/source_sell_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::{
    ExecutionSourceSellOutcome as Outcome, ExecutionSourceSellReject as Reject,
    SourceSellCandidate as Candidate,
};
use fixture::*;

#[test]
fn batch_identity_no_capture_p_q_and_mutations_preserve_first_generation() -> Result<()> {
    for mode in ["unknown", "known"] {
        let db = Db::new()?;
        db.proven("buy", "source-a")?;
        let e = db.sell("batch", "source-a");
        let p = db.position()?;
        let mut mutated = e.clone();
        mutated.ts_utc += chrono::Duration::seconds(10);
        let candidates = vec![
            if mode == "known" {
                Some(Candidate::new(&e, &p))
            } else {
                None
            },
            Some(Candidate::new(&e, "different-Q")),
            Some(Candidate::new(&mutated, "different-Q")),
        ];
        let out = db
            .store
            .insert_observed_swaps_with_candidates(&[e.clone(), e.clone(), mutated], &candidates)?;
        assert_eq!(out.inserted, vec![true, false, false]);
        let h = db.store.load_source_sell_handoff(&e.signature)?.unwrap();
        assert_eq!(
            h.original_position_id,
            if mode == "known" { Some(p) } else { None }
        );
        assert_eq!(h.event.ts_utc, e.ts_utc);
        assert_eq!(
            h.disposition,
            if mode == "known" {
                "pending"
            } else {
                "unknown"
            }
        );
        assert_eq!(db.store.load_observed_swaps_since(db.now)?.len(), 1);
        let before = snapshot(&db.conn()?, &[])?;
        assert!(!db.store.insert_observed_swap(&e)?);
        assert_eq!(snapshot(&db.conn()?, &[])?, before);
    }
    Ok(())
}

#[test]
fn candidate_payload_is_bound_and_old_observed_cannot_be_backfilled() -> Result<()> {
    let db = Db::new()?;
    db.proven("buy", "source-a")?;
    let e = db.sell("a", "source-a");
    let c = Candidate::new(&e, &db.position()?);
    let mut changed = e.clone();
    changed.exact_amounts.as_mut().unwrap().amount_in_raw = "4001".into();
    let before = snapshot(&db.conn()?, &[])?;
    assert!(db
        .store
        .insert_observed_swaps_with_candidates(&[changed], &[Some(c.clone())])
        .is_err());
    assert_eq!(snapshot(&db.conn()?, &[])?, before);
    db.store.insert_observed_swap(&e)?;
    // Explicit historical fixture: pre0065 observed row lacks its handoff.
    db.conn()?.execute(
        "DELETE FROM source_sell_handoffs WHERE signature=?1",
        [&e.signature],
    )?;
    assert_eq!(
        db.store
            .insert_observed_swaps_with_candidates(&[e.clone()], &[Some(c)])?
            .inserted,
        vec![false]
    );
    let h = db.store.load_source_sell_handoff(&e.signature)?.unwrap();
    assert_eq!(h.disposition, "unknown");
    assert!(h.original_position_id.is_none());
    Ok(())
}

#[test]
fn handoff_and_commit_faults_rollback_entire_observed_activity_batch() -> Result<()> {
    for capture in [false, true] {
        for fault in ["abort", "ignore", "commit"] {
            let db = Db::new()?;
            db.proven("buy", "source-a")?;
            let a = db.sell("good-a", "source-a");
            let b = db.sell("fault-b", "source-a");
            let sql=match fault {
                "abort"=>"CREATE TRIGGER fault BEFORE INSERT ON source_sell_handoffs WHEN NEW.signature='fault-b' BEGIN SELECT RAISE(ABORT,'handoff refused'); END;",
                "ignore"=>"CREATE TRIGGER fault BEFORE INSERT ON source_sell_handoffs WHEN NEW.signature='fault-b' BEGIN SELECT RAISE(IGNORE); END;",
                _=>"CREATE TABLE parent(id PRIMARY KEY); CREATE TABLE effect(id REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fault AFTER INSERT ON source_sell_handoffs WHEN NEW.signature='fault-b' BEGIN INSERT INTO effect VALUES(42); END;",
            };
            db.conn()?.execute_batch(sql)?;
            let before = snapshot(&db.conn()?, &[])?;
            let cs = if capture {
                vec![
                    Some(Candidate::new(&a, &db.position()?)),
                    Some(Candidate::new(&b, &db.position()?)),
                ]
            } else {
                vec![None, None]
            };
            assert!(
                db.store
                    .insert_observed_swaps_with_candidates(&[a, b], &cs)
                    .is_err(),
                "{capture}/{fault}"
            );
            assert_eq!(snapshot(&db.conn()?, &[])?, before, "{capture}/{fault}");
        }
    }
    Ok(())
}

#[test]
fn capture_before_commit_keeps_p_and_refuses_q_then_fresh_q_continues() -> Result<()> {
    let mut db = Db::new()?;
    db.proven("old", "source-a")?;
    let e = db.sell("old-exit", "source-a");
    let p = db.position()?;
    let c = Candidate::new(&e, &p);
    db.store
        .record_execution_canary_manual_terminal_write_off("mint", "tiny", "close", db.now)?;
    db.proven("new", "source-a")?;
    let q = db.position()?;
    assert_ne!(p, q);
    db.store
        .insert_observed_swaps_with_candidates(&[e.clone()], &[Some(c)])?; // Reply deliberately discarded.
    db.reopen()?;
    assert_eq!(
        db.store
            .load_source_sell_handoff(&e.signature)?
            .unwrap()
            .original_position_id,
        Some(p)
    );
    let before = snapshot(&db.conn()?, &["source_sell_handoffs"])?;
    assert!(matches!(
        db.store.process_source_sell_handoff(&e.signature)?,
        Outcome::Rejected(Reject::GenerationMismatch)
    ));
    assert_eq!(snapshot(&db.conn()?, &["source_sell_handoffs"])?, before);
    assert_eq!(db.position()?, q);
    let fresh = db.sell("fresh", "source-a");
    db.store.insert_observed_swaps_with_candidates(
        &[fresh.clone()],
        &[Some(Candidate::new(&fresh, &q))],
    )?;
    assert_eq!(
        inserted(db.store.process_source_sell_handoff(&fresh.signature)?).position_id,
        q
    );
    Ok(())
}

#[test]
fn every_identity_field_is_immutable_after_retention_and_known_cannot_be_replaced() -> Result<()> {
    for field in 0..11 {
        let db = Db::new()?;
        db.proven("buy", "source-a")?;
        let e = db.sell("exit", "source-a");
        let p = db.position()?;
        db.store
            .insert_observed_swaps_with_candidates(&[e.clone()], &[Some(Candidate::new(&e, &p))])?;
        inserted(db.store.process_source_sell_handoff(&e.signature)?);
        db.store
            .record_execution_canary_manual_terminal_write_off("mint", "tiny", "close", db.now)?;
        assert_eq!(
            db.store
                .delete_observed_swaps_before_batch(e.ts_utc + chrono::Duration::seconds(1), 1)?,
            1
        );
        let mut changed = e.clone();
        match field {
            0 => changed.wallet = "another-source".into(),
            1 => changed.dex = "another-dex".into(),
            2 => changed.token_in = "another-mint".into(),
            3 => changed.token_out = "another-output".into(),
            4 => changed.amount_in += 1.0,
            5 => changed.amount_out += 1.0,
            6 => changed.slot += 1,
            7 => changed.ts_utc += chrono::Duration::days(1),
            8 => changed.exact_amounts.as_mut().unwrap().amount_in_raw = "9999".into(),
            9 => changed.exact_amounts.as_mut().unwrap().amount_out_decimals = 8,
            _ => changed.exact_amounts = None,
        }
        let before = snapshot(&db.conn()?, &[])?;
        assert_eq!(
            db.store
                .insert_observed_swaps_with_candidates(
                    &[changed.clone()],
                    &[Some(Candidate::new(&changed, "new-q"))]
                )?
                .inserted,
            vec![false]
        );
        assert_eq!(
            snapshot(&db.conn()?, &[])?,
            before,
            "identity field {field}"
        );
        assert!(
            db.store
                .insert_observed_swaps_with_candidates(
                    &[e.clone()],
                    &[Some(Candidate::new(&e, "new-q"))]
                )?
                .inserted[0]
        );
        assert_eq!(
            db.store
                .load_source_sell_handoff(&e.signature)?
                .unwrap()
                .original_position_id,
            Some(p)
        );
    }
    Ok(())
}

#[test]
fn ignored_canonical_insert_cannot_commit_an_orphan_known_handoff() -> Result<()> {
    let db = Db::new()?;
    let e = db.sell("ignored", "source-a");
    db.conn()?.execute_batch(
        "CREATE TRIGGER fault BEFORE INSERT ON observed_swaps BEGIN SELECT RAISE(IGNORE); END;",
    )?;
    let before = snapshot(&db.conn()?, &[])?;
    assert!(db
        .store
        .insert_observed_swaps_with_candidates(&[e.clone()], &[Some(Candidate::new(&e, "p"))])
        .is_err());
    assert_eq!(snapshot(&db.conn()?, &[])?, before);
    Ok(())
}
