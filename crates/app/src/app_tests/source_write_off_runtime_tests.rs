use super::{source_write_off_fixture::*, source_write_off_http::QuoteServer};
use crate::execution_canary_route::{
    process_failed_sell_simulation_sweep, process_tiny_submit_sell_quote_event_for_route,
};
use anyhow::{Context, Result};

async fn terminal_a_cannot_close_b(simulation: bool) -> Result<()> {
    let f = Fixture::new(7000)?;
    let order = fail_order(&f.store, &f.signal.signal_id, ROUTE, f.now, simulation, 1)?;
    f.replace(7000)?;
    let b = f
        .store
        .load_execution_canary_open_position("mint")?
        .unwrap();
    assert_ne!(b.position_id, f.staged.position_id);
    let before = snapshot(&f.conn()?, &[])?;
    let c = config("http://127.0.0.1:9");
    let summary = if simulation {
        process_failed_sell_simulation_sweep(&c, &f.store, f.now).await?
    } else {
        process_tiny_submit_sell_quote_event_for_route(&c, &f.store, EVENT, f.now).await?
    }
    .context("runtime summary")?;
    assert_eq!(
        summary.last_order_id.as_deref(),
        Some(order.order_id.as_str())
    );
    assert_eq!(summary.sell_closed, 0, "old A wrote off new B");
    assert!(summary
        .last_error
        .as_deref()
        .unwrap()
        .contains(&order.order_id));
    assert_eq!(
        summary.skipped_reason,
        Some("source_sell_generation_mismatch")
    );
    assert_eq!(snapshot(&f.conn()?, &[])?, before);
    assert_eq!(
        f.store.load_execution_canary_open_position("mint")?,
        Some(b)
    );
    Ok(())
}
#[tokio::test]
async fn source_terminal_simulation_a_cannot_write_off_equal_ts_b() -> Result<()> {
    terminal_a_cannot_close_b(true).await
}
#[tokio::test]
async fn source_terminal_no_route_a_cannot_write_off_equal_ts_b() -> Result<()> {
    terminal_a_cannot_close_b(false).await
}

#[tokio::test]
async fn source_dust_quote_await_cannot_write_off_replacement_b() -> Result<()> {
    let f = Fixture::new(1)?;
    let mut server = QuoteServer::start(&f, true).await?;
    let outcome = process_tiny_submit_sell_quote_event_for_route(
        &config(&server.url),
        &f.store,
        EVENT,
        f.now,
    )
    .await;
    server.finish().await?;
    let summary = outcome?.context("dust summary")?;
    let order = f
        .store
        .load_execution_canary_order_by_signal(&f.signal.signal_id)?
        .unwrap();
    assert_eq!(
        summary.last_order_id.as_deref(),
        Some(order.order_id.as_str())
    );
    assert_eq!(
        summary.failed, 0,
        "stale quote failure must not overwrite A"
    );
    assert_eq!(summary.source_sell_refusals.count(), 1);
    assert_eq!(summary.sell_closed, 0, "old dust A wrote off replacement B");
    assert!(order.err_code.is_none());
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CANDIDATE
    );
    let b = f
        .store
        .load_execution_canary_open_position("mint")?
        .context("B must survive")?;
    assert_ne!(b.position_id, f.staged.position_id);
    assert_eq!(b.qty_exact.unwrap().raw(), 1);
    assert_eq!(b.cost_lamports.unwrap().as_u64(), 1000);
    assert_eq!(
        snapshot_without_order(&f.conn()?, &order.order_id)?,
        server.after_quote_mutation.lock().unwrap().clone().unwrap()
    );
    assert!(summary
        .last_error
        .as_deref()
        .unwrap()
        .contains(&order.order_id));
    assert_eq!(
        summary.skipped_reason,
        Some("source_sell_generation_mismatch")
    );
    Ok(())
}

#[tokio::test]
async fn source_terminal_correct_promoted_and_legacy_write_off_once() -> Result<()> {
    for simulation in [true, false] {
        for promoted in [true, false] {
            let f = Fixture::new(7000)?;
            if !promoted {
                f.conn()?
                    .execute("DELETE FROM execution_source_sell_promotions", [])?;
            }
            let order = fail_order(&f.store, &f.signal.signal_id, ROUTE, f.now, simulation, 1)?;
            let untouched = snapshot(&f.conn()?, &["orders", "positions"])?;
            let c = config("http://127.0.0.1:9");
            let summary = if simulation {
                process_failed_sell_simulation_sweep(&c, &f.store, f.now).await?
            } else {
                process_tiny_submit_sell_quote_event_for_route(&c, &f.store, EVENT, f.now).await?
            }
            .context("terminal result")?;
            assert_written_off(&f, &summary, &order.order_id, false, simulation)?;
            assert_eq!(snapshot(&f.conn()?, &["orders", "positions"])?, untouched);
            let after = snapshot(&f.conn()?, &[])?;
            let replay = if simulation {
                process_failed_sell_simulation_sweep(&c, &f.store, f.now).await?
            } else {
                process_tiny_submit_sell_quote_event_for_route(&c, &f.store, EVENT, f.now).await?
            };
            assert_eq!(replay.map_or(0, |s| s.sell_closed), 0);
            assert_eq!(snapshot(&f.conn()?, &[])?, after);
        }
    }
    Ok(())
}

#[tokio::test]
async fn source_dust_correct_promoted_and_legacy_write_off_once() -> Result<()> {
    for promoted in [true, false] {
        let f = Fixture::new(1)?;
        if !promoted {
            f.conn()?
                .execute("DELETE FROM execution_source_sell_promotions", [])?;
        }
        let untouched = snapshot(&f.conn()?, &["orders", "positions"])?;
        let mut server = QuoteServer::start(&f, false).await?;
        let mut c = config(&server.url);
        c.max_submit_attempts = 5;
        let result =
            process_tiny_submit_sell_quote_event_for_route(&c, &f.store, EVENT, f.now).await;
        server.finish().await?;
        let summary = result?.context("dust result")?;
        let order = f
            .store
            .load_execution_canary_order_by_signal(&f.signal.signal_id)?
            .unwrap();
        assert_eq!(order.attempt, 1);
        assert_written_off(&f, &summary, &order.order_id, true, false)?;
        assert_eq!(snapshot(&f.conn()?, &["orders", "positions"])?, untouched);
        let after = snapshot(&f.conn()?, &[])?;
        let reopened = copybot_storage_core::SqliteStore::open(&f.path)?;
        let replay =
            process_tiny_submit_sell_quote_event_for_route(&c, &reopened, EVENT, f.now).await?;
        assert_eq!(replay.map_or(0, |s| s.sell_closed), 0);
        assert_eq!(snapshot(&f.conn()?, &[])?, after);
    }
    Ok(())
}

fn assert_written_off(
    f: &Fixture,
    s: &crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary,
    id: &str,
    dust: bool,
    simulation: bool,
) -> Result<()> {
    use copybot_storage_core::*;
    assert_eq!(s.last_order_id.as_deref(), Some(id));
    assert_eq!(s.sell_closed, 1);
    assert_eq!(s.sell_dust_closed, usize::from(dust));
    assert_eq!(s.last_closed_qty, if dust { 0.001 } else { 7.0 });
    assert_eq!(s.last_pnl_sol, -0.000001);
    assert_eq!(s.open_positions, 0);
    assert_eq!(s.signing_envelope_built, 0);
    assert!(s.last_submit_idempotency_key.is_none());
    let order = f.store.load_execution_canary_order(id)?.unwrap();
    assert!(order.tx_signature.is_none());
    assert_eq!(
        order.err_code.as_deref(),
        Some(if simulation {
            EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED
        } else {
            EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE
        })
    );
    let reason = if simulation {
        "terminal_failed_sell_simulation_written_off"
    } else {
        "terminal_failed_sell_no_route_written_off"
    };
    assert_eq!(s.skipped_reason, Some(reason));
    assert!(order.simulation_error.unwrap().starts_with(reason));
    assert!(f
        .store
        .load_execution_canary_open_position("mint")?
        .is_none());
    let row: (String, String, i64, i64) = f.conn()?.query_row(
        "SELECT state,qty_raw,cost_lamports,pnl_lamports FROM positions WHERE position_id=?1",
        [&f.staged.position_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    assert_eq!(row, ("closed".into(), "0".into(), 0, -1000));
    Ok(())
}

#[tokio::test]
async fn source_terminal_refusal_and_storage_errors_stay_local_to_order_a() -> Result<()> {
    for simulation in [true, false] {
        for sql in [
            "UPDATE execution_source_sell_promotions SET signal_id='moved-marker';",
            "DELETE FROM execution_canary_receipt_facts WHERE side='buy';",
            "CREATE TRIGGER injected BEFORE UPDATE OF state ON positions BEGIN SELECT RAISE(ABORT,'close_failure'); END;",
            "CREATE TABLE injected_parent(id TEXT PRIMARY KEY); CREATE TABLE injected_child(id TEXT REFERENCES injected_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER injected AFTER UPDATE OF state ON positions BEGIN INSERT INTO injected_child VALUES('absent'); END;",
        ] {
            let f = Fixture::new(7000)?;
            let order = fail_order(&f.store, &f.signal.signal_id, ROUTE, f.now, simulation, 1)?;
            f.conn()?.execute_batch(sql)?;
            let before = snapshot(&f.conn()?, &[])?;
            let c = config("http://127.0.0.1:9");
            let s = if simulation { process_failed_sell_simulation_sweep(&c, &f.store, f.now).await? }
                else { process_tiny_submit_sell_quote_event_for_route(&c, &f.store, EVENT, f.now).await? }.context("local refusal must return summary")?;
            assert_eq!(s.last_order_id.as_deref(), Some(order.order_id.as_str()));
            assert_eq!(s.sell_closed, 0);
            assert_eq!(s.last_closed_qty, 0.0);
            assert_eq!(s.last_pnl_sol, 0.0);
            assert!(s.skipped_reason.unwrap().starts_with("source_sell_"));
            assert!(s.last_error.unwrap().contains(&order.order_id));
            assert_eq!(snapshot(&f.conn()?, &[])?, before);
        }
    }
    Ok(())
}

#[tokio::test]
async fn source_refused_a_does_not_block_valid_b_on_the_same_store() -> Result<()> {
    use copybot_storage_core::ExecutionSourceSellPromotionOutcome;
    let f = Fixture::new(7000)?;
    let a = fail_order(&f.store, &f.signal.signal_id, ROUTE, f.now, false, 1)?;
    f.replace(7000)?;
    let c = config("http://127.0.0.1:9");
    let rejected = process_tiny_submit_sell_quote_event_for_route(&c, &f.store, EVENT, f.now)
        .await?
        .unwrap();
    assert_eq!(rejected.sell_closed, 0);
    assert!(rejected.last_error.unwrap().contains(&a.order_id));
    let b = staged(&f.store, "sell-b", f.now)?;
    let ExecutionSourceSellPromotionOutcome::Inserted(binding) =
        f.store.promote_execution_source_sell_intent(&b.intent_id)?
    else {
        anyhow::bail!("expected B promotion");
    };
    let signal = f
        .store
        .load_copy_signal_by_signal_id(&binding.signal_id)?
        .unwrap();
    let mut event = quote(&signal, f.now);
    event.event_id = "quote:source-write-off-b".into();
    f.store.record_execution_quote_canary_event(&event)?;
    let order_b = fail_order(&f.store, &signal.signal_id, ROUTE, f.now, false, 1)?;
    let accepted =
        process_tiny_submit_sell_quote_event_for_route(&c, &f.store, &event.event_id, f.now)
            .await?
            .unwrap();
    assert_eq!(
        accepted.last_order_id.as_deref(),
        Some(order_b.order_id.as_str())
    );
    assert_eq!(accepted.sell_closed, 1);
    assert_eq!(accepted.last_closed_qty, 7.0);
    assert_eq!(accepted.last_pnl_sol, -0.000001);
    assert!(f
        .store
        .load_execution_canary_open_position("mint")?
        .is_none());
    assert_eq!(f.store.load_execution_canary_order(&a.order_id)?, Some(a));
    Ok(())
}

// R1 adds only traversal metadata. Keep all pre-existing business-row assertions,
// excluding precisely that permitted checkpoint (verified independently in cursor tests).
fn snapshot(
    conn: &rusqlite::Connection,
    exclude: &[&str],
) -> Result<std::collections::BTreeMap<String, Vec<String>>> {
    let mut exclude = exclude.to_vec();
    exclude.push("execution_failed_sell_sweep_cursors");
    super::source_write_off_fixture::snapshot(conn, &exclude)
}
