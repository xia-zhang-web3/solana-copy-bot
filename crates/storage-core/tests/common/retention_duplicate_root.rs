//! Root probe: include as an external storage-core integration target only; no production wiring.
#[path = "source_sell_fixture.rs"]
mod fixture;
use crate::backend::SqliteStore;
use anyhow::Result;
use chrono::Duration;
use fixture::{inserted, Db};

fn run(mutate_duplicate_time: bool) -> Result<()> {
    let mut d = Db::new()?;
    d.proven("buy", "source-a")?;
    let a = d.sell("pin", "source-a");
    let mut b = d.sell("deleted", "other");
    b.ts_utc += Duration::seconds(1);
    let floor = a.ts_utc + Duration::seconds(10);
    let store = SqliteStore::open(&d.path)?;
    store.insert_recent_raw_journal_batch(&[a.clone(), b], floor)?;
    inserted(
        d.store
            .stage_execution_source_sell_intent(&a, &d.position()?)?,
    );
    assert_eq!(
        store.prune_recent_raw_journal_before_batch(floor, 1, floor)?,
        1
    );
    assert_eq!(
        store
            .recent_raw_journal_state_cached_read_only_required()?
            .covered_since,
        None
    );
    let mut late = a.clone();
    late.signature = "late-old".into();
    let mut duplicate = a.clone();
    if mutate_duplicate_time {
        duplicate.ts_utc = floor + Duration::seconds(10);
    }
    let summary =
        store.insert_recent_raw_journal_batch(&[late, duplicate], floor + Duration::seconds(20))?;
    assert_eq!(summary.inserted_rows, 1);
    assert_eq!(summary.row_count, 2);
    let physical: i64 = d.conn()?.query_row(
        "SELECT count(*) FROM observed_swaps WHERE ts>=?1",
        [floor.to_rfc3339()],
        |r| r.get(0),
    )?;
    assert_eq!(physical, 0);
    let physical_pin_ts: String = d.conn()?.query_row(
        "SELECT ts FROM observed_swaps WHERE signature='pin'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(
        physical_pin_ts,
        a.ts_utc.to_rfc3339(),
        "duplicate was ignored"
    );
    drop(store);
    d.reopen()?;
    let reader = SqliteStore::open_read_only(&d.path)?;
    let cached = reader.recent_raw_journal_state_cached_read_only_required()?;
    let recomputed = reader.recent_raw_journal_state_read_only()?;
    assert_eq!(recomputed.row_count, 2);
    assert_eq!(recomputed.covered_since, None);
    assert_eq!(cached.covered_since, None,
            "ignored duplicate time must not bridge removed range; altered={} cached={:?} recomputed={:?}",
            mutate_duplicate_time, cached, recomputed);
    assert_eq!(
        cached.covered_through_cursor,
        recomputed.covered_through_cursor
    );
    Ok(())
}
#[test]
fn root_exact_duplicate_control() -> Result<()> {
    run(false)
}
#[test]
fn root_ignored_duplicate_time_does_not_prove_coverage() -> Result<()> {
    run(true)
}
