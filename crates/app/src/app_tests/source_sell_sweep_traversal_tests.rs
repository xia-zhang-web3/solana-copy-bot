use super::source_sell_sweep_fixture::*;
use crate::execution_canary_route::process_failed_sell_simulation_sweep;
use anyhow::{Context, Result};
use copybot_storage_core::SqliteStore;

#[tokio::test]
async fn sweep_reaches_b_beyond_equal_time_prefix_despite_newer_a_reopen_and_quote_duplicates(
) -> Result<()> {
    for simulation in [true, false] {
        let count = 19; // More than two complete eight-visit budgets.
        let p = Prefix::new(count, simulation, false)?;
        let c = config("http://127.0.0.1:9");
        for i in 0..40 {
            let mut event = quote(&p.old[count - 1], p.f.now);
            event.event_id = format!("duplicate:{i:03}");
            p.f.store.record_execution_quote_canary_event(&event)?;
        }
        let mut reached = false;
        for tick in 0..4 {
            p.later_a(count + tick, simulation)?;
            let a_rows = p.f.conn()?.prepare("SELECT order_id,err_code,attempt FROM orders WHERE order_id<>?1 ORDER BY order_id")?
                .query_map([&p.b.order_id], |r|Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,i64>(2)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let before = no_business_change(&p.f)?;
            let reopened = SqliteStore::open(&p.f.path)?;
            let s = process_failed_sell_simulation_sweep(&c, &reopened, p.f.now)
                .await?
                .context("sweep summary")?;
            assert!(
                s.source_sell_write_off_refusals.count() <= 8,
                "bounded per invocation"
            );
            if s.sell_closed == 0 {
                assert_eq!(no_business_change(&p.f)?, before);
            } else {
                assert_eq!(s.sell_closed, 1);
                assert_eq!(s.last_order_id.as_deref(), Some(p.b.order_id.as_str()));
                let after = p.f.conn()?.prepare("SELECT order_id,err_code,attempt FROM orders WHERE order_id<>?1 ORDER BY order_id")?
                    .query_map([&p.b.order_id], |r|Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,i64>(2)?)))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                assert_eq!(after, a_rows);
                reached = true;
                break;
            }
        }
        assert!(
            reached,
            "restarts/new heads must not reset the blocked prefix"
        );
        assert!(p
            .f
            .store
            .load_execution_canary_open_position("mint")?
            .is_none());
        let money = no_business_change(&p.f)?;
        for _ in 0..3 {
            let reopened = SqliteStore::open(&p.f.path)?;
            let s = process_failed_sell_simulation_sweep(&c, &reopened, p.f.now)
                .await?
                .unwrap();
            assert_eq!(s.sell_closed, 0);
            assert_eq!(no_business_change(&p.f)?, money);
        }
    }
    Ok(())
}

#[tokio::test]
async fn failed_build_prefix_cannot_starve_simulation_and_missing_cursor_row_does_not_reset_traversal(
) -> Result<()> {
    let p = Prefix::new(18, false, true)?;
    let c = config("http://127.0.0.1:9");
    let first = process_failed_sell_simulation_sweep(&c, &p.f.store, p.f.now)
        .await?
        .unwrap();
    assert_eq!(first.sell_closed, 0);
    assert_eq!(first.source_sell_write_off_refusals.count(), 8);
    // Explicit retention-style deletion AFTER real A proof/selection, never seed fabrication.
    let boundary: i64 = p.f.conn()?.query_row(
        "SELECT last_rowid FROM execution_failed_sell_sweep_cursors WHERE route=?1",
        [ROUTE],
        |r| r.get(0),
    )?;
    p.f.conn()?
        .execute("DELETE FROM orders WHERE rowid=?1", [boundary])?;
    let mut closed = 0;
    for _ in 0..3 {
        let reopened = SqliteStore::open(&p.f.path)?;
        let s = process_failed_sell_simulation_sweep(&c, &reopened, p.f.now)
            .await?
            .unwrap();
        closed += s.sell_closed;
        if s.sell_closed > 0 {
            assert_eq!(s.last_order_id.as_deref(), Some(p.b.order_id.as_str()));
        }
    }
    assert_eq!(closed, 1);
    Ok(())
}

#[tokio::test]
async fn wrap_retries_a_after_temporary_local_fault_is_removed() -> Result<()> {
    for simulation in [true, false] {
        let f = Fixture::new(7000)?;
        let a = fail_order(&f.store, &f.signal.signal_id, ROUTE, f.now, simulation, 1)?;
        f.conn()?.execute_batch("CREATE TRIGGER temporary_fault BEFORE UPDATE OF err_code ON orders BEGIN SELECT RAISE(ABORT,'temporary_local_fault'); END;")?;
        let before = no_business_change(&f)?;
        let c = config("http://127.0.0.1:9");
        let s = process_failed_sell_simulation_sweep(&c, &f.store, f.now)
            .await?
            .unwrap();
        assert_eq!(s.sell_closed, 0);
        assert_eq!(s.source_sell_write_off_refusals.count(), 1);
        assert_eq!(s.source_sell_write_off_refusals.order_id(), a.order_id);
        assert_eq!(no_business_change(&f)?, before);
        f.conn()?.execute_batch("DROP TRIGGER temporary_fault;")?;
        let reopened = SqliteStore::open(&f.path)?;
        let s = process_failed_sell_simulation_sweep(&c, &reopened, f.now)
            .await?
            .unwrap();
        assert_eq!(s.sell_closed, 1);
        assert_eq!(s.last_order_id.as_deref(), Some(a.order_id.as_str()));
    }
    Ok(())
}

#[tokio::test]
async fn local_fault_a_survives_b_success_in_one_actual_production_event() -> Result<()> {
    for simulation in [true, false] {
        let f = Fixture::new(7000)?;
        let a = fail_order(
            &f.store,
            &f.signal.signal_id,
            ROUTE,
            f.now + chrono::Duration::seconds(2),
            simulation,
            1,
        )?;
        let b_signal = add_signal(&f, "same-generation-b")?;
        let b = fail_order(
            &f.store,
            &b_signal.signal_id,
            ROUTE,
            f.now + chrono::Duration::seconds(1),
            simulation,
            1,
        )?;
        f.conn()?.execute_batch(&format!("CREATE TRIGGER fail_only_a BEFORE UPDATE OF err_code ON orders WHEN OLD.order_id='{}' BEGIN SELECT RAISE(ABORT,'SYNTHETIC_PRIVATE_PAYLOAD'); END;",a.order_id))?;
        let c = config("http://127.0.0.1:9");
        let state = process_failed_sell_simulation_sweep(&c, &f.store, f.now)
            .await?
            .unwrap();
        assert_eq!(state.sell_closed, 1);
        assert_eq!(state.last_order_id.as_deref(), Some(b.order_id.as_str()));
        assert_eq!(state.source_sell_write_off_refusals.order_id(), a.order_id);
        assert_eq!(
            f.store.load_execution_canary_order(&a.order_id)?,
            Some(a.clone())
        );
        let mut tick = crate::execution_canary::ExecutionCanaryTickSummary::default();
        crate::execution_canary_summary::apply_state_machine_summary(&mut tick, state);
        assert!(tick.has_status_change());
        let event = super::submit_refusal_fixture::capture(|| {
            crate::telemetry::record_execution_canary_tick(&tick)
        });
        assert_eq!(event["source_sell_write_off_refusals"], "1");
        assert_eq!(event["source_sell_write_off_refusal_order_id"], a.order_id);
        assert_eq!(
            event["source_sell_write_off_refusal_reason"],
            "source_sell_write_off_unavailable"
        );
        assert_eq!(event["last_state_machine_order_id"], b.order_id);
        assert!(event["state_machine_skipped_reason"].ends_with("_written_off"));
        assert!(event
            .values()
            .all(|value| !value.contains("SYNTHETIC_PRIVATE_PAYLOAD")));
        eprintln!("B34_R1_EVENT {}", serde_json::to_string(&event)?);
    }
    Ok(())
}

#[tokio::test]
async fn global_database_failure_surfaces_instead_of_successful_queue_progress() -> Result<()> {
    for broken in ["cursor", "proof"] {
        let f = Fixture::new(7000)?;
        let a = fail_order(&f.store, &f.signal.signal_id, ROUTE, f.now, false, 1)?;
        let sql = if broken == "cursor" {
            "DROP TABLE execution_failed_sell_sweep_cursors;"
        } else {
            "ALTER TABLE execution_canary_receipt_proofs RENAME TO hidden_receipt_proofs;"
        };
        f.conn()?.execute_batch(sql)?;
        let before = no_business_change(&f)?;
        let error =
            process_failed_sell_simulation_sweep(&config("http://127.0.0.1:9"), &f.store, f.now)
                .await
                .expect_err("global schema failure");
        assert!(format!("{error:#}").contains("no such table"));
        assert_eq!(no_business_change(&f)?, before);
        assert_eq!(f.store.load_execution_canary_order(&a.order_id)?, Some(a));
    }
    Ok(())
}
