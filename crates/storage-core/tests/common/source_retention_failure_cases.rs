use super::*;

#[test]
fn schema_and_metadata_errors_never_become_unpinned_delete() -> Result<()> {
    for prune in [false, true] {
        for fault in [
            "DROP TABLE execution_source_sell_intents",
            "ALTER TABLE execution_source_sell_intents RENAME TO original_staging; CREATE VIEW execution_source_sell_intents AS SELECT * FROM original_staging",
            "ALTER TABLE execution_source_sell_intents RENAME COLUMN position_id TO missing_position",
            "ALTER TABLE positions RENAME COLUMN state TO missing_state",
            "ALTER TABLE schema_migrations RENAME COLUMN version TO missing_version",
            "DROP TABLE observed_retention_boundary",
            "DELETE FROM observed_retention_boundary",
            "UPDATE observed_retention_boundary SET floor_ts='invalid'",
            "ALTER TABLE observed_retention_boundary RENAME TO original_boundary; CREATE VIEW observed_retention_boundary AS SELECT * FROM original_boundary",
            "ALTER TABLE recent_raw_journal_state RENAME COLUMN row_count TO missing_count",
        ] {
            let d = Db::new()?; d.proven("buy", "source-a")?;
            let store = SqliteStore::open(&d.path)?;
            let a = d.sell("a", "source-a");
            store.insert_recent_raw_journal_batch(&[a.clone()], a.ts_utc)?;
            inserted(d.store.stage_execution_source_sell_intent(&a, &d.position()?)?);
            let conn = d.conn()?; conn.execute_batch(fault)?;
            let before = snapshot(&conn, &[])?;
            let cutoff = a.ts_utc + Duration::seconds(1);
            let outcome = if prune { store.prune_recent_raw_journal_before_batch(cutoff, 1, cutoff) }
                else { store.delete_observed_swaps_before_batch(cutoff, 1) };
            assert!(outcome.is_err(), "fault must propagate: {fault}: {outcome:?}");
            assert_eq!(snapshot(&conn, &[])?, before, "{fault}");
        }
    }
    Ok(())
}

#[test]
fn delete_boundary_metadata_and_commit_faults_roll_back_slice() -> Result<()> {
    for prune in [false, true] {
        for trigger in [
            "CREATE TRIGGER fault BEFORE DELETE ON observed_swaps BEGIN SELECT RAISE(ABORT,'delete-fault'); END;",
            "CREATE TRIGGER fault BEFORE UPDATE ON observed_retention_boundary BEGIN SELECT RAISE(ABORT,'boundary-fault'); END;",
            "CREATE TRIGGER fault BEFORE UPDATE ON observed_retention_boundary BEGIN SELECT RAISE(IGNORE); END;",
            "CREATE TRIGGER fault AFTER UPDATE ON observed_retention_boundary BEGIN DELETE FROM observed_retention_boundary; END;",
            "CREATE TRIGGER fault BEFORE UPDATE ON recent_raw_journal_state BEGIN SELECT RAISE(ABORT,'state-fault'); END;",
            "CREATE TRIGGER fault BEFORE UPDATE ON recent_raw_journal_state BEGIN SELECT RAISE(IGNORE); END;",
            "CREATE TABLE parent(id PRIMARY KEY); CREATE TABLE effect(id REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fault AFTER DELETE ON observed_swaps BEGIN INSERT INTO effect VALUES(123); END;",
        ] {
            let mut d = Db::new()?;
            let store = SqliteStore::open(&d.path)?;
            let a = d.sell("delete", "source-a");
            store.insert_recent_raw_journal_batch(&[a.clone()], a.ts_utc)?;
            let conn = d.conn()?; conn.execute_batch(trigger)?;
            let before = snapshot(&conn, &[])?;
            let cutoff = a.ts_utc + Duration::seconds(1);
            let outcome = if prune { store.prune_recent_raw_journal_before_batch(cutoff, 1, cutoff) }
                else { store.delete_observed_swaps_before_batch(cutoff, 1) };
            assert!(outcome.is_err(), "{trigger}: {outcome:?}");
            assert_eq!(snapshot(&conn, &[])?, before);
            drop(store); d.reopen()?;
            assert_eq!(snapshot(&conn, &[])?, before);
        }
    }
    Ok(())
}

#[test]
fn invalid_boundary_rejects_late_append_and_bulk_without_partial_insert() -> Result<()> {
    for bulk in [false, true] {
        let d = Db::new()?;
        let store = SqliteStore::open(&d.path)?;
        let old = d.sell("old", "source");
        let cutoff = old.ts_utc + Duration::seconds(1);
        store.insert_recent_raw_journal_batch(&[old.clone()], cutoff)?;
        store.prune_recent_raw_journal_before_batch(cutoff, 1, cutoff)?;
        let c = d.conn()?;
        c.execute(
            "UPDATE observed_retention_boundary SET floor_ts='broken'",
            [],
        )?;
        let before = snapshot(&c, &[])?;
        if bulk {
            assert!(store
                .insert_recent_raw_journal_batch_bulk_with_deadline(
                    &[old],
                    cutoff,
                    std::time::Instant::now() + std::time::Duration::from_secs(5)
                )
                .is_err());
        } else {
            assert!(store
                .insert_recent_raw_journal_batch(&[old], cutoff)
                .is_err());
        }
        assert_eq!(snapshot(&c, &[])?, before);
    }
    Ok(())
}
