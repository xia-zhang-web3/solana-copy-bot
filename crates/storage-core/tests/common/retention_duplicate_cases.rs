#[path = "source_sell_fixture.rs"]
mod fixture;
use crate::backend::SqliteStore;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage_core::RecentRawJournalWriteSummary;
use fixture::{inserted, snapshot, Db};

fn append(
    store: &SqliteStore,
    rows: &[SwapEvent],
    at: DateTime<Utc>,
    bulk: bool,
) -> Result<RecentRawJournalWriteSummary> {
    if bulk {
        let (summary, exhausted) = store.insert_recent_raw_journal_batch_bulk_with_deadline(
            rows,
            at,
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        )?;
        assert!(!exhausted);
        Ok(summary)
    } else {
        store.insert_recent_raw_journal_batch(rows, at)
    }
}

fn scenario(bulk: bool, with_late: bool, with_fresh: bool) -> Result<()> {
    let mut d = Db::new()?;
    d.proven("buy", "source-a")?;
    let pin = d.sell("pin", "source-a");
    let mut removed = d.sell("removed", "other");
    removed.ts_utc += Duration::seconds(1);
    let floor = pin.ts_utc + Duration::seconds(10);
    let store = SqliteStore::open(&d.path)?;
    append(&store, &[pin.clone(), removed], floor, bulk)?;
    inserted(
        d.store
            .stage_execution_source_sell_intent(&pin, &d.position()?)?,
    );
    assert_eq!(
        store.prune_recent_raw_journal_before_batch(floor, 1, floor)?,
        1
    );
    let before = store.recent_raw_journal_state_cached_read_only_required()?;
    let before_pin = d.conn()?.query_row(
        "SELECT ts,slot,qty_in FROM observed_swaps WHERE signature='pin'",
        [],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, f64>(2)?,
            ))
        },
    )?;
    let mut rows = Vec::new();
    if with_late {
        let mut late = pin.clone();
        late.signature = "late".into();
        rows.push(late);
    }
    let mut fresh = pin.clone();
    fresh.signature = "fresh".into();
    fresh.ts_utc = floor + Duration::seconds(5);
    if with_fresh {
        rows.push(fresh.clone());
    }
    let mut duplicate = pin.clone();
    duplicate.ts_utc = floor + Duration::seconds(100);
    duplicate.slot += 999;
    duplicate.amount_in += 10.0;
    rows.push(duplicate.clone());
    let at = floor + Duration::seconds(101);
    let expected_inserted = usize::from(with_late) + usize::from(with_fresh);
    let summary = append(&store, &rows, at, bulk)?;
    assert_eq!(summary.inserted_rows, expected_inserted);
    assert_eq!(summary.row_count, 1 + expected_inserted);
    assert_eq!(summary.covered_since, with_fresh.then_some(fresh.ts_utc));
    assert_eq!(
        summary.covered_through_cursor.as_ref().unwrap().signature,
        if with_fresh { "fresh" } else { "pin" }
    );
    let expected = if with_fresh { &fresh } else { &pin };
    assert_eq!(
        summary.covered_through_cursor.as_ref().unwrap().ts_utc,
        expected.ts_utc
    );
    assert_eq!(
        summary.covered_through_cursor.as_ref().unwrap().slot,
        expected.slot
    );
    // A duplicate-only replay after fresh evidence must also leave the physical cursor unchanged.
    let replay = append(&store, &[duplicate], at, bulk)?;
    assert_eq!(replay.inserted_rows, 0);
    assert_eq!(replay.covered_since, summary.covered_since);
    assert_eq!(
        replay.covered_through_cursor,
        summary.covered_through_cursor
    );
    assert_eq!(replay.row_count, summary.row_count);
    drop(store);
    d.reopen()?;
    let c = d.conn()?;
    assert_eq!(
        c.query_row(
            "SELECT ts,slot,qty_in FROM observed_swaps WHERE signature='pin'",
            [],
            |r| Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, f64>(2)?
            ))
        )?,
        before_pin
    );
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM observed_swaps WHERE ts>=?1",
            [floor.to_rfc3339()],
            |r| r.get::<_, usize>(0)
        )?,
        usize::from(with_fresh)
    );
    assert_eq!(
        c.query_row("SELECT count(*) FROM observed_swaps", [], |r| r
            .get::<_, usize>(0))?,
        summary.row_count
    );
    // Persisted metadata itself must be correct; a reader-side clamp alone is insufficient.
    assert_eq!(c.query_row("SELECT covered_since_ts,covered_through_cursor_signature,row_count,covered_through_cursor_ts,covered_through_cursor_slot FROM recent_raw_journal_state WHERE id=1", [], |r|Ok((r.get::<_,Option<String>>(0)?,r.get::<_,String>(1)?,r.get::<_,usize>(2)?,r.get::<_,String>(3)?,r.get::<_,u64>(4)?)))?,
        (summary.covered_since.map(|ts|ts.to_rfc3339()),summary.covered_through_cursor.as_ref().unwrap().signature.clone(),summary.row_count,expected.ts_utc.to_rfc3339(),expected.slot));
    let before_read = snapshot(&c, &[])?;
    let reader = SqliteStore::open_read_only(&d.path)?;
    for state in [
        reader.recent_raw_journal_state_cached_read_only_required()?,
        reader.recent_raw_journal_state_read_only()?,
    ] {
        assert_eq!(state.row_count, summary.row_count);
        assert_eq!(state.covered_since, summary.covered_since);
        assert_eq!(state.covered_through_cursor, summary.covered_through_cursor);
        assert_eq!(
            (state.last_pruned_at, state.last_pruned_rows),
            (before.last_pruned_at, before.last_pruned_rows)
        );
        assert_eq!(
            (state.last_batch_rows, state.last_batch_completed_at),
            (0, Some(at))
        );
    }
    assert_eq!(snapshot(&c, &[])?, before_read);
    let writable = SqliteStore::open(&d.path)?;
    for state in [
        writable.recent_raw_journal_state_cached()?,
        writable.recent_raw_journal_state()?,
    ] {
        assert_eq!(state.covered_since, summary.covered_since);
        assert_eq!(state.covered_through_cursor, summary.covered_through_cursor);
    }
    Ok(())
}

#[test]
fn duplicate_only_cannot_invent_physical_cursor_after_retention() -> Result<()> {
    for bulk in [false, true] {
        scenario(bulk, false, false)?;
    }
    Ok(())
}
#[test]
fn late_new_row_and_ignored_last_input_do_not_bridge_deleted_range() -> Result<()> {
    for bulk in [false, true] {
        scenario(bulk, true, false)?;
    }
    Ok(())
}
#[test]
fn legitimate_new_row_still_proves_coverage_despite_ignored_last_input() -> Result<()> {
    for bulk in [false, true] {
        scenario(bulk, true, true)?;
    }
    Ok(())
}
