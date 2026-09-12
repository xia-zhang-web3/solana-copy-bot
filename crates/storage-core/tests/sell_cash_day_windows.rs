#[path = "common/sell_cash_day_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use fixture::*;

#[test]
fn accounting_days_keep_partials_in_their_original_day_after_close() -> Result<()> {
    let db = Db::new(3, 9, 0, 1, -4)?;
    let yesterday = time("2026-09-05T23:59:59.999999998Z");
    first(&db, yesterday)?;
    let old = sums(&db, yesterday + Duration::nanoseconds(1), 1, -7, 7)?;
    sums(&db, as_of(), 0, 0, 0)?;
    next(
        &db,
        "exec-canary:today-profit",
        1,
        6,
        time("2026-09-06T00:00:00Z"),
    )?;
    next(
        &db,
        "exec-canary:today-final",
        1,
        1,
        time("2026-09-06T01:00:00Z"),
    )?;
    let today = sums(&db, as_of(), 2, 1, 2)?;
    assert_eq!(
        (
            today.known_events.partial_events,
            today.known_events.full_events
        ),
        (1, 1)
    );
    assert_eq!(view(&db, yesterday + Duration::nanoseconds(1))?, old);
    sums(&db, time("2026-09-07T12:00:00Z"), 0, 0, 0)?;
    Ok(())
}

#[test]
fn half_open_day_uses_chrono_nanoseconds_and_equivalent_offsets() -> Result<()> {
    let since = time("2026-09-06T00:00:00Z");
    let end = time("2026-09-06T12:00:00.000000001Z");
    let times = [
        since - Duration::nanoseconds(1),
        since,
        since + Duration::nanoseconds(1),
        end - Duration::nanoseconds(1),
        end,
        end + Duration::nanoseconds(1),
    ];
    let db = Db::new(6, 6, 0, 1, 0)?;
    for (i, at) in times.into_iter().enumerate() {
        if i == 0 {
            first(&db, at)?;
        } else {
            next(&db, &format!("exec-canary:boundary-{i}"), 1, 0, at)?;
        }
    }
    let expected = sums(&db, end, 3, -3, 3)?;
    assert_eq!(expected.since, "2026-09-06T00:00:00+00:00");
    // Only timestamp representation changes; amounts/fills remain actual writer output.
    for (id, offset) in [
        ("exec-canary:boundary-1", "2026-09-06T03:00:00+03:00"),
        ("exec-canary:boundary-3", "2026-09-06T08:00:00-04:00"),
        (
            "exec-canary:boundary-4",
            "2026-09-06T15:00:00.000000001+03:00",
        ),
    ] {
        db.conn()?.execute(
            "UPDATE fills SET settlement_ts=?2 WHERE order_id=?1",
            [id, offset],
        )?;
    }
    assert_eq!(view(&db, end)?, expected);
    sums(&db, since, 0, 0, 0)?;
    sums(&db, since + Duration::nanoseconds(1), 1, -1, 1)?;
    Ok(())
}

#[test]
fn old_chain_times_late_accounting_and_replay_do_not_move_the_event() -> Result<()> {
    let db = Db::new(2, 6, 0, 1, 0)?;
    db.conn()?.execute_batch(
        "UPDATE orders SET submit_ts='2026-09-01T00:00:00Z',confirm_ts='2026-09-01T00:00:01Z';
        UPDATE execution_canary_receipt_proofs SET confirmed_at='2026-09-01T00:00:01Z';",
    )?;
    let before = sums(&db, as_of(), 0, 0, 0)?;
    assert_eq!(
        before
            .undated_obligations
            .confirmed_unreconciled_without_fill,
        1
    );
    let accounted = time("2026-09-06T11:00:00.123456789Z");
    first(&db, accounted)?;
    sums(&db, time("2026-09-01T12:00:00Z"), 0, 0, 0)?;
    let original = sums(&db, as_of(), 1, -3, 3)?;
    let frozen = snapshot(&db.conn()?)?;
    let fresh = db
        .store
        .load_execution_canary_receipt_facts(ORDER)?
        .unwrap();
    let reopened = copybot_storage_core::SqliteStore::open(&db.path)?;
    assert!(
        reopened
            .apply_execution_canary_sell_settlement(&fresh, time("2026-09-10T12:00:00Z"))?
            .already_accounted
    );
    assert_eq!(snapshot(&db.conn()?)?, frozen);
    assert_eq!(view(&db, as_of())?, original);
    sums(&db, time("2026-09-10T12:00:00Z"), 0, 0, 0)?;
    let saved: String = db.conn()?.query_row(
        "SELECT settlement_ts FROM fills WHERE order_id=?1",
        [ORDER],
        |r| r.get(0),
    )?;
    assert_eq!(time(&saved), accounted);
    Ok(())
}
