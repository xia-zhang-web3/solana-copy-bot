#[path = "source_sell_fixture.rs"]
mod fixture;
use crate::backend::SqliteStore;
use anyhow::Result;
use chrono::Duration;
use copybot_core_types::SwapEvent;
use copybot_storage_core::SourceSellCandidate as Candidate;
use fixture::*;
use std::time::{Duration as StdDuration, Instant};

fn write(store: &SqliteStore, e: &SwapEvent, mode: usize) -> Result<()> {
    match mode {
        0 => {
            store.insert_observed_swap(e)?;
        }
        1 => {
            store.insert_observed_swaps_batch(&[e.clone()])?;
        }
        2 => {
            store.insert_observed_swaps_batch_with_activity_days_measured(&[e.clone()])?;
        }
        3 => {
            store.insert_recent_raw_journal_batch(&[e.clone()], e.ts_utc)?;
        }
        _ => {
            store.insert_recent_raw_journal_batch_bulk_with_deadline(
                &[e.clone()],
                e.ts_utc,
                Instant::now() + StdDuration::from_secs(5),
            )?;
        }
    }
    Ok(())
}

#[test]
fn every_main_writer_preserves_unknown_through_retention_reinsert_and_mutation() -> Result<()> {
    for mode in 0..5 {
        let mut db = Db::new()?;
        let e = db.sell("observed-only", "source-a");
        let store = SqliteStore::open(&db.path)?;
        write(&store, &e, mode)?;
        let h = db.store.load_source_sell_handoff(&e.signature)?.unwrap();
        assert_eq!(h.disposition, "unknown");
        assert!(h.original_position_id.is_none());
        let cutoff = e.ts_utc + Duration::seconds(1);
        assert_eq!(store.delete_observed_swaps_before_batch(cutoff, 1)?, 1);
        drop(store);
        db.reopen()?;
        db.proven("new-position", "source-a")?;
        let mut changed = e.clone();
        changed.ts_utc += Duration::days(1);
        let out = db.store.insert_observed_swaps_with_candidates(
            &[changed],
            &[Some(Candidate::new(&e, &db.position()?))],
        );
        assert!(out.is_err(), "candidate identity must be immutable");
        let store = SqliteStore::open(&db.path)?;
        let mut changed = e.clone();
        changed.ts_utc += Duration::days(1);
        write(&store, &changed, mode)?;
        assert!(
            db.store.load_observed_swaps_since(db.now)?.is_empty(),
            "mutated replay cannot recreate raw: {mode}"
        );
        write(&store, &e, mode)?;
        let again = db.store.load_source_sell_handoff(&e.signature)?.unwrap();
        assert_eq!(format!("{h:?}"), format!("{again:?}"));
        assert!(db.store.advance_source_sell_handoff()?.is_none());
        assert_eq!(db.store.load_observed_swaps_since(db.now)?.len(), 1);
        if mode >= 3 {
            let state = store.recent_raw_journal_state_cached()?;
            assert!(
                state
                    .covered_through_cursor
                    .as_ref()
                    .is_none_or(|c| c.ts_utc <= e.ts_utc),
                "{state:?}"
            );
        }
    }
    Ok(())
}

#[test]
fn pending_pin_precedes_limit_even_after_p_closes_and_disposition_releases_it() -> Result<()> {
    for prune in [false, true] {
        let db = Db::new()?;
        db.proven("p", "source-a")?;
        let e = db.sell("pending", "source-a");
        db.store.insert_observed_swaps_with_candidates(
            &[e.clone()],
            &[Some(Candidate::new(&e, &db.position()?))],
        )?;
        db.store
            .record_execution_canary_manual_terminal_write_off("mint", "tiny", "close", db.now)?;
        let store = SqliteStore::open(&db.path)?;
        let mut b = db.sell("other", "other");
        b.ts_utc += Duration::seconds(1);
        store.insert_observed_swap(&b)?; // Main DB has no separate journal cached row count; prune bootstraps it.
        let cutoff = b.ts_utc + Duration::seconds(1);
        let deleted = if prune {
            store.prune_recent_raw_journal_before_batch(cutoff, 1, cutoff)?
        } else {
            store.delete_observed_swaps_before_batch(cutoff, 1)?
        };
        assert_eq!(deleted, 1);
        assert_eq!(
            db.store.load_observed_swaps_since(db.now)?[0].signature,
            e.signature
        );
        db.store.process_source_sell_handoff(&e.signature)?;
        assert_eq!(
            db.store
                .load_source_sell_handoff(&e.signature)?
                .unwrap()
                .disposition,
            "refused"
        );
        assert_eq!(store.delete_observed_swaps_before_batch(cutoff, 1)?, 1);
        assert!(
            db.store.load_source_sell_handoff(&e.signature)?.is_some(),
            "terminal tombstone retained"
        );
    }
    Ok(())
}

#[test]
fn recorded_schema_damage_is_error_in_all_writers_recovery_and_retention() -> Result<()> {
    for sql in [
        "DROP TRIGGER source_sell_handoff_after_observed",
        "DROP TRIGGER source_sell_handoff_before_observed",
        "DROP TRIGGER source_sell_handoff_before_retention",
        "ALTER TABLE source_sell_handoffs RENAME TO lost_handoffs",
        "ALTER TABLE source_sell_handoff_cursor RENAME COLUMN last_sequence TO broken",
        "DROP INDEX idx_source_sell_handoff_pending",
    ] {
        let db = Db::new()?;
        let e = db.sell("event", "source-a");
        db.conn()?.execute_batch(sql)?;
        let store = SqliteStore::open(&db.path)?;
        for mode in 0..5 {
            assert!(write(&store, &e, mode).is_err(), "{mode}/{sql}");
        }
        assert!(db.store.advance_source_sell_handoff().is_err(), "{sql}");
        assert!(
            store
                .delete_observed_swaps_before_batch(e.ts_utc, 1)
                .is_err(),
            "{sql}"
        );
        assert!(db.store.load_observed_swaps_since(db.now)?.is_empty());
    }
    Ok(())
}

#[test]
fn pre65_observed_without_handoff_retains_unknown_when_cleanup_happens_before_replay() -> Result<()>
{
    let mut db = Db::new()?;
    let e = db.sell("pre65-old", "source-a");
    db.store.insert_observed_swap(&e)?;
    // Exact pre0065 state: canonical raw exists, but the additive handoff was never populated.
    db.conn()?.execute(
        "DELETE FROM source_sell_handoffs WHERE signature=?1",
        [&e.signature],
    )?;
    let store = SqliteStore::open(&db.path)?;
    assert_eq!(
        store.delete_observed_swaps_before_batch(e.ts_utc + Duration::seconds(1), 1)?,
        1
    );
    drop(store);
    db.reopen()?;
    db.proven("current-q", "source-a")?;
    db.store.insert_observed_swaps_with_candidates(
        &[e.clone()],
        &[Some(Candidate::new(&e, &db.position()?))],
    )?;
    let handoff = db.store.load_source_sell_handoff(&e.signature)?.unwrap();
    assert!(
        handoff.original_position_id.is_none(),
        "retention must retain the old Unknown: {handoff:?}"
    );
    assert_eq!(handoff.disposition, "unknown");
    Ok(())
}

#[test]
fn changed_literal_refuses_every_public_writer_and_retention_without_effects() -> Result<()> {
    for (object, needle, replacement) in [
        (
            "source_sell_handoff_after_observed",
            "NEW.token_out='So",
            "NEW.token_out=' So",
        ),
        (
            "source_sell_handoff_before_observed",
            "token_out='So",
            "token_out='So\t",
        ),
        (
            "source_sell_handoff_before_retention",
            "OLD.token_out='So",
            "OLD.token_out='So\n",
        ),
    ] {
        let mut db = Db::new()?;
        let e = db.sell("literal-damage", "source-a");
        let conn = db.conn()?;
        let (kind, original): (String, String) = conn.query_row(
            "SELECT type,sql FROM sqlite_master WHERE name=?1",
            [object],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!(original.matches(needle).count(), 1);
        conn.execute_batch(&format!("DROP {kind} {object}"))?;
        conn.execute_batch(&original.replacen(needle, replacement, 1))?;
        db.reopen()?;
        assert!(copybot_storage_core::source_sell_handoff_schema::available(&db.conn()?).is_err());
        let store = SqliteStore::open(&db.path)?;
        let before = snapshot(&db.conn()?, &[])?;
        for mode in 0..5 {
            assert!(write(&store, &e, mode).is_err(), "{object}/{mode}");
            assert_eq!(snapshot(&db.conn()?, &[])?, before, "{object}/{mode}");
        }
        assert!(store
            .delete_observed_swaps_before_batch(e.ts_utc, 1)
            .is_err());
        assert!(db.store.advance_source_sell_handoff().is_err());
        assert_eq!(snapshot(&db.conn()?, &[])?, before);
    }
    Ok(())
}
