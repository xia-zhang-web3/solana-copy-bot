use super::source_write_off_fixture::*;
use crate::execution_canary_route::process_failed_sell_simulation_sweep;
use anyhow::{Context, Result};
use copybot_storage_core::{ExecutionSourceSellPromotionOutcome, EXECUTION_STATUS_CANARY_FAILED};

async fn actual_sweep_reaches_b(simulation: bool, with_blocker: bool) -> Result<()> {
    let f = Fixture::new(7000)?;
    // A's later failed attempt is first in the production DESC retry ordering.
    let a = if with_blocker {
        Some(fail_order(
            &f.store,
            &f.signal.signal_id,
            ROUTE,
            f.now + chrono::Duration::seconds(2),
            simulation,
            1,
        )?)
    } else {
        None
    };
    f.replace(7000)?;
    let b = staged(&f.store, "sell-b", f.now)?;
    let ExecutionSourceSellPromotionOutcome::Inserted(binding) =
        f.store.promote_execution_source_sell_intent(&b.intent_id)?
    else {
        anyhow::bail!("B promotion must succeed");
    };
    let signal_b = f
        .store
        .load_copy_signal_by_signal_id(&binding.signal_id)?
        .unwrap();
    let mut event_b = quote(&signal_b, f.now);
    event_b.event_id = "quote:sweep-review-b".into();
    f.store.record_execution_quote_canary_event(&event_b)?;
    let order_b = fail_order(
        &f.store,
        &signal_b.signal_id,
        ROUTE,
        f.now + chrono::Duration::seconds(1),
        simulation,
        1,
    )?;
    let config = config("http://127.0.0.1:9");
    let mut reached = false;
    let mut selected = Vec::new();
    for _ in 0..3 {
        let summary = process_failed_sell_simulation_sweep(
            &config,
            &f.store,
            f.now + chrono::Duration::seconds(3),
        )
        .await?
        .context("sweep must return summary")?;
        selected.push((
            summary.last_order_id.clone(),
            summary.skipped_reason,
            summary.sell_closed,
        ));
        if let Some(a) = &a {
            assert_eq!(
                f.store.load_execution_canary_order(&a.order_id)?,
                Some(a.clone()),
                "A must remain refused without terminal/money mutation"
            );
        }
        if summary.last_order_id.as_deref() == Some(&order_b.order_id) && summary.sell_closed == 1 {
            reached = true;
            break;
        }
    }
    eprintln!("simulation={simulation}, blocker={with_blocker}, selected={selected:?}");
    let saved_b = f
        .store
        .load_execution_canary_order(&order_b.order_id)?
        .unwrap();
    assert_eq!(saved_b.status, EXECUTION_STATUS_CANARY_FAILED);
    assert!(
        reached,
        "actual retry sweep must reach valid B despite retained A; selected={selected:?}"
    );
    assert!(f
        .store
        .load_execution_canary_open_position("mint")?
        .is_none());
    let (state, pnl): (String, i64) = f.conn()?.query_row(
        "SELECT state,pnl_lamports FROM positions WHERE position_id=?1",
        [&b.position_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!((state.as_str(), pnl), ("closed", -1000));
    Ok(())
}

#[tokio::test]
async fn root_simulation_sweep_must_advance_past_refused_a() -> Result<()> {
    actual_sweep_reaches_b(true, true).await
}
#[tokio::test]
async fn root_no_route_sweep_must_advance_past_refused_a() -> Result<()> {
    actual_sweep_reaches_b(false, true).await
}
#[tokio::test]
async fn root_sweep_valid_b_without_blocker_control() -> Result<()> {
    for simulation in [true, false] {
        actual_sweep_reaches_b(simulation, false).await?;
    }
    Ok(())
}
