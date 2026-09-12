use super::*;

fn readers(
    d: &Db,
    count: usize,
    floor: chrono::DateTime<chrono::Utc>,
    only_old: bool,
) -> Result<()> {
    let store = SqliteStore::open(&d.path)?;
    let conn = d.conn()?;
    let schema = snapshot(&conn, &[])?;
    assert_eq!(
        store
            .recent_raw_journal_state_cached_read_only_required()?
            .covered_through_cursor,
        store
            .recent_raw_journal_state_read_only()?
            .covered_through_cursor,
        "cached physical cursor must not retain a deleted latest row"
    );
    for state in [
        store.recent_raw_journal_state_read_only()?,
        store.recent_raw_journal_state_cached_read_only_required()?,
    ] {
        assert_eq!(state.row_count, count);
        if only_old {
            assert_eq!(state.covered_since, None, "old pins are no coverage");
        } else {
            assert!(
                state.covered_since.is_some_and(|since| since >= floor),
                "{state:?}"
            );
        }
    }
    assert_eq!(
        snapshot(&conn, &[])?,
        schema,
        "read-only readers cannot perform DDL or repair"
    );
    Ok(())
}

#[test]
fn coverage_floor_survives_late_append_duplicate_close_and_reopen() -> Result<()> {
    for prune in [false, true] {
        let mut d = Db::new()?;
        d.proven("buy", "source-a")?;
        let a = d.sell("old-pin", "source-a");
        let mut b = d.sell("old-delete", "other");
        b.ts_utc += Duration::seconds(1);
        let floor = a.ts_utc + Duration::seconds(10);
        let store = SqliteStore::open(&d.path)?;
        store.insert_recent_raw_journal_batch(&[a.clone(), b], floor)?;
        inserted(
            d.store
                .stage_execution_source_sell_intent(&a, &d.position()?)?,
        );
        let removed = if prune {
            store.prune_recent_raw_journal_before_batch(floor, 1, floor)?
        } else {
            store.delete_observed_swaps_before_batch(floor, 1)?
        };
        assert_eq!(removed, 1);
        drop(store);
        d.reopen()?;
        readers(&d, 1, floor, true)?;
        let store = SqliteStore::open(&d.path)?;
        let duplicate = store.insert_recent_raw_journal_batch(&[a.clone()], floor)?;
        assert_eq!(duplicate.inserted_rows, 0);
        assert_eq!(duplicate.covered_since, None);
        let mut late = a.clone();
        late.signature = "late-old".into();
        let summary = store.insert_recent_raw_journal_batch(&[late.clone()], floor)?;
        assert_eq!(summary.row_count, 2);
        assert_eq!(summary.covered_since, None);
        readers(&d, 2, floor, true)?;
        d.close()?; // Removing pin eligibility alone must not lower the durable floor.
        d.reopen()?;
        readers(&d, 2, floor, true)?;
        let mut fresh = a.clone();
        fresh.signature = "fresh".into();
        fresh.ts_utc = floor + Duration::seconds(10);
        let summary = store.insert_recent_raw_journal_batch(&[fresh.clone()], fresh.ts_utc)?;
        assert_eq!(summary.row_count, 3);
        assert_eq!(
            summary.covered_through_cursor.unwrap().signature,
            fresh.signature
        );
        readers(&d, 3, floor, false)?;
        let mut later_old = late;
        later_old.signature = "later-old".into();
        let summary = store
            .insert_recent_raw_journal_batch_bulk_with_deadline(
                &[later_old],
                fresh.ts_utc,
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            )?
            .0;
        assert_eq!(summary.row_count, 4);
        assert!(summary.covered_since.unwrap() >= floor);
        drop(store);
        d.reopen()?;
        readers(&d, 4, floor, false)?;
        let store = SqliteStore::open(&d.path)?;
        assert_eq!(
            store.prune_recent_raw_journal_before_batch(floor, 10, fresh.ts_utc)?,
            3
        );
        readers(&d, 1, floor, false)?;
        let state = store.recent_raw_journal_state_cached_read_only_required()?;
        assert_eq!(
            (state.last_pruned_rows, state.last_pruned_at),
            (3, Some(fresh.ts_utc))
        );
        assert_eq!(
            state.covered_through_cursor.unwrap().signature,
            fresh.signature
        );
    }
    Ok(())
}

#[test]
fn old_schema_and_discovery_bootstrap_need_no_execution_tables() -> Result<()> {
    for old_schema in [false, true] {
        for prune in [false, true] {
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("old.db");
            let mut store = SqliteStore::open(&path)?;
            if old_schema {
                let migrations = dir.path().join("pre0058");
                copy_migrations_before(&migrations, "0058")?;
                store.run_migrations(&migrations)?;
            }
            store.ensure_recent_raw_journal_tables()?;
            let d = Db::new()?;
            let event = d.sell("old", "source");
            store.insert_recent_raw_journal_batch(&[event.clone()], event.ts_utc)?;
            let cutoff = event.ts_utc + Duration::seconds(1);
            let n = if prune {
                store.prune_recent_raw_journal_before_batch(cutoff, 1, cutoff)?
            } else {
                store
                    .delete_observed_swaps_before_batched(cutoff, 1)?
                    .deleted_rows
            };
            assert_eq!(n, 1);
            drop(store);
            let conn = rusqlite::Connection::open(&path)?;
            assert!(!conn
                .prepare("SELECT 1 FROM sqlite_master WHERE name='execution_source_sell_intents'")?
                .exists([])?);
            if !old_schema {
                assert!(!conn
                    .prepare("SELECT 1 FROM sqlite_master WHERE name='positions'")?
                    .exists([])?);
            }
            assert_eq!(
                conn.query_row(
                    "SELECT floor_ts FROM observed_retention_boundary WHERE id=1",
                    [],
                    |r| r.get::<_, String>(0)
                )?,
                cutoff.to_rfc3339()
            );
            let schema_before: String =
                conn.query_row("SELECT group_concat(sql) FROM sqlite_master", [], |r| {
                    r.get(0)
                })?;
            let reader = SqliteStore::open_read_only(&path)?;
            assert_eq!(
                reader.recent_raw_journal_state_read_only()?.covered_since,
                None
            );
            assert_eq!(
                schema_before,
                conn.query_row("SELECT group_concat(sql) FROM sqlite_master", [], |r| {
                    r.get::<_, String>(0)
                })?
            );
        }
    }
    Ok(())
}

#[test]
fn boundary_upgrade_is_additive_and_keeps_existing_floor_on_repeat() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("upgrade.db");
    let old = dir.path().join("pre0063");
    copy_migrations_before(&old, "0063")?;
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&old)?;
    let conn = rusqlite::Connection::open(&path)?;
    let before = snapshot(&conn, &["schema_migrations"])?;
    let through_63 = dir.path().join("through-0063");
    copy_migrations_before(&through_63, "0064")?; // This fixture owns only the 0063 boundary.
    assert_eq!(store.run_migrations(&through_63)?, 1);
    assert_eq!(
        snapshot(&conn, &["schema_migrations", "observed_retention_boundary"])?,
        before
    );
    assert_eq!(
        conn.query_row(
            "SELECT floor_ts FROM observed_retention_boundary",
            [],
            |r| r.get::<_, Option<String>>(0)
        )?,
        None
    );
    drop(store);
    let mut store = SqliteStore::open(&path)?;
    assert_eq!(store.run_migrations(&through_63)?, 0);
    Ok(())
}

#[test]
fn discovery_bootstrap_preserves_nonnull_floor_on_reopen_and_detects_loss() -> Result<()> {
    for prune in [false, true] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("bootstrap.db");
        let mut store = SqliteStore::open(&path)?;
        let d = Db::new()?;
        let event = d.sell("old", "source");
        let cutoff = event.ts_utc + Duration::seconds(1);
        store.insert_recent_raw_journal_batch(&[event.clone()], cutoff)?;
        if prune {
            store.prune_recent_raw_journal_before_batch(cutoff, 1, cutoff)?;
        } else {
            store.delete_observed_swaps_before_batched(cutoff, usize::MAX)?;
        }
        let conn = rusqlite::Connection::open(&path)?;
        let before = conn.query_row(
            "SELECT floor_ts FROM observed_retention_boundary",
            [],
            |r| r.get::<_, String>(0),
        )?;
        // Discovery bootstrap is not a full legacy schema upgrade. Run only this additive migration.
        let boundary_migrations = dir.path().join("boundary-only");
        std::fs::create_dir(&boundary_migrations)?;
        let migration = "0063_observed_retention_boundary.sql";
        std::fs::copy(
            std::path::Path::new(MIGRATIONS).join(migration),
            boundary_migrations.join(migration),
        )?;
        assert_eq!(store.run_migrations(&boundary_migrations)?, 0);
        conn.execute_batch(include_str!(
            "../../../../migrations/0063_observed_retention_boundary.sql"
        ))?;
        drop(store);
        let store = SqliteStore::open(&path)?;
        assert_eq!(
            before,
            conn.query_row(
                "SELECT floor_ts FROM observed_retention_boundary",
                [],
                |r| r.get::<_, String>(0)
            )?
        );
        assert_eq!(before, cutoff.to_rfc3339());
        store.insert_recent_raw_journal_batch(&[event], cutoff)?;
        conn.execute_batch("DROP TABLE observed_retention_boundary")?;
        let before = snapshot(&conn, &[])?;
        let reader = SqliteStore::open_read_only(&path)?;
        assert!(reader.recent_raw_journal_state_read_only().is_err());
        assert!(reader
            .recent_raw_journal_state_cached_read_only_required()
            .is_err());
        assert!(store.delete_observed_swaps_before_batch(cutoff, 1).is_err());
        assert_eq!(snapshot(&conn, &[])?, before);
    }
    Ok(())
}
