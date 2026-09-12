#[path = "common/source_sell_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::{observed_retention::restrict_coverage, RecentRawJournalStateRow};
use fixture::{inserted, Db};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn measure(c: &rusqlite::Connection, count: usize) -> Result<(usize, RecentRawJournalStateRow)> {
    let steps = Arc::new(AtomicUsize::new(0));
    let captured = steps.clone();
    let mut state = RecentRawJournalStateRow {
        row_count: count,
        ..Default::default()
    };
    c.progress_handler(
        1,
        Some(move || {
            captured.fetch_add(1, Ordering::Relaxed);
            false
        }),
    );
    restrict_coverage(c, &mut state)?;
    c.progress_handler(0, None::<fn() -> bool>);
    assert_eq!(
        state.row_count, count,
        "endpoint refresh must not recount or alter physical count"
    );
    Ok((steps.load(Ordering::Relaxed), state))
}

#[test]
fn actual_post_retention_endpoint_refresh_uses_bounded_index_lookups() -> Result<()> {
    let d = Db::new()?;
    d.proven("buy", "source-a")?;
    let pin = d.sell("pin", "source-a");
    let mut b = d.sell("b", "other");
    b.ts_utc += Duration::seconds(1);
    let floor = pin.ts_utc + Duration::seconds(10);
    d.store
        .insert_recent_raw_journal_batch(&[pin.clone(), b], floor)?;
    inserted(
        d.store
            .stage_execution_source_sell_intent(&pin, &d.position()?)?,
    );
    assert_eq!(
        d.store
            .prune_recent_raw_journal_before_batch(floor, 1, floor)?,
        1
    );
    let c = d.conn()?;
    let (small, state) = measure(&c, 1)?;
    assert_eq!(state.covered_since, None);
    let source = include_str!("../src/observed_retention_coverage.rs");
    let queries = source
        .split('"')
        .filter(|s| s.starts_with("SELECT ts"))
        .collect::<Vec<_>>();
    assert_eq!(
        queries.len(),
        2,
        "read the actual endpoint SQL, not a copied plan"
    );
    for sql in queries {
        let args = if sql.contains("?1") {
            vec![floor.to_rfc3339()]
        } else {
            vec![]
        };
        let plan = c
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
            .query_map(rusqlite::params_from_iter(args), |r| r.get::<_, String>(3))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert!(
            plan.iter()
                .all(|p| p.contains("COVERING INDEX idx_observed_swaps_ts_slot_signature")),
            "{plan:?}"
        );
        eprintln!("B52R1_ENDPOINT_PLAN {plan:?}");
    }
    c.execute(
        "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<20000)
        INSERT INTO observed_swaps(signature,wallet_id,dex,token_in,token_out,qty_in,qty_out,
            qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals,slot,ts)
        SELECT 'fresh-'||x,wallet_id,dex,token_in,token_out,qty_in,qty_out,
            qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals,slot,?1
        FROM n CROSS JOIN observed_swaps WHERE signature='pin'",
        [(floor + Duration::seconds(1)).to_rfc3339()],
    )?;
    let (large, state) = measure(&c, 20001)?;
    assert_eq!(state.covered_since, Some(floor + Duration::seconds(1)));
    assert_eq!(
        state.covered_through_cursor.unwrap().ts_utc,
        floor + Duration::seconds(1)
    );
    assert!(
        large < small * 2,
        "indexed endpoints must not scan growing row history: {small}/{large}"
    );
    eprintln!("B52R1_ENDPOINT_VM physical_rows=1 steps={small}; physical_rows=20001 steps={large}");
    Ok(())
}
