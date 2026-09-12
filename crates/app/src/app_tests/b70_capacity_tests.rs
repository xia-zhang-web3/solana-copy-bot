use super::{b70_job_fixture::Jobs, *};
use anyhow::{ensure, Result};
use std::sync::Arc;

fn input(f: &Jobs, n: usize, sell: bool) -> crate::shadow_scheduler::ShadowTaskInput {
    let mut swap = f.swap.clone();
    swap.signature = format!("shadow-budget-{n}");
    if sell {
        std::mem::swap(&mut swap.token_in, &mut swap.token_out);
    }
    crate::shadow_scheduler::ShadowTaskInput {
        swap,
        follow_snapshot: Arc::new(FollowSnapshot::default()),
        key: crate::shadow_scheduler::ShadowTaskKey {
            wallet: format!("wallet-{n}"),
            token: format!("token-{n}"),
        },
    }
}

#[tokio::test]
async fn b70_shared_active_pending_held_sell_inline_and_saturation() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let f = Jobs::new(&format!("http://{}", listener.local_addr()?))?;
    let mut scheduler = ShadowScheduler::new();
    for _ in 0..4 {
        scheduler.shadow_workers.spawn(std::future::pending());
    }
    scheduler
        .admit_hot_quote(f.admission()?)
        .map_err(|_| anyhow::anyhow!("active slot"))?;
    ensure!(scheduler.active_task_count() == 5);
    ensure!(!scheduler.should_process_shadow_inline(true, false, 4, &input(&f, 1, false).key));
    scheduler
        .hold_sell_for_causality(256, input(&f, 0, true), 60_000, Utc::now())
        .map_err(|_| anyhow::anyhow!("held slot"))?;
    for n in 1..255 {
        scheduler
            .enqueue_shadow_task(256, input(&f, n, false))
            .map_err(|_| anyhow::anyhow!("pending slot"))?;
    }
    let queued = f.another(2)?;
    let queued_signal = queued.origin.signal_id();
    scheduler
        .admit_hot_quote(queued)
        .map_err(|_| anyhow::anyhow!("last shared slot"))?;
    ensure!(scheduler.buffered_shadow_task_count() == 256 && scheduler.hot_quotes.pending() == 1);
    let rejected = f.another(3)?;
    let rejected_signal = rejected.origin.signal_id();
    let rejected = scheduler
        .admit_hot_quote(rejected)
        .err()
        .context("must reject full queue")?;
    drop(rejected);
    ensure!(!f.runner.entry_pending(&rejected_signal));
    scheduler.spawn_shadow_tasks_up_to_limit(
        &f.base.path.to_string_lossy(),
        &ShadowService::new(permissive_shadow_quality()),
        usize::MAX,
    );
    ensure!(scheduler.active_task_count() == 5);
    // Resetting shadow bookkeeping must not clear either quote claim.
    scheduler.shadow_scheduler_needs_reset = true;
    prepare_shadow_scheduler_before_select(
        &f.base.store,
        &f.base.path.to_string_lossy(),
        &ShadowService::new(permissive_shadow_quality()),
        &mut scheduler,
        &Default::default(),
        &mut Default::default(),
        &mut Default::default(),
        &mut Default::default(),
    )?;
    ensure!(f.runner.entry_pending(&queued_signal));
    ExecutionCanaryRunner::new(f.config.clone())
        .make_room_for_owned_or_shadow_sell(&f.base.store, &mut scheduler)?;
    ensure!(!f.runner.entry_pending(&queued_signal));
    ensure!(scheduler.held_shadow_sell_count() == 1 && scheduler.pending_shadow_task_count == 254);
    scheduler
        .enqueue_shadow_task(256, input(&f, 255, true))
        .map_err(|_| anyhow::anyhow!("SELL slot"))?;
    ensure!(scheduler.buffered_shadow_task_count() == 256 && scheduler.active_task_count() == 5);
    let event = f
        .base
        .store
        .load_latest_execution_quote_canary_entry_event(&queued_signal)?
        .unwrap();
    ensure!(event.error.as_deref() == Some("hot_quote_evicted_for_sell"));
    scheduler.hot_quotes.shutdown().await;
    scheduler.shadow_workers.abort_all();
    while let Some(result) = scheduler.shadow_workers.join_next().await {
        match result {
            Err(e) => ensure!(e.is_cancelled()),
            Ok(_) => anyhow::bail!("unexpected shadow completion"),
        }
    }
    ensure!(scheduler.active_task_count() == 0);
    super::b64_http_fixture::no_more(&listener).await?;
    Ok(())
}

#[tokio::test]
async fn b70_capacity_refusal_is_recorded_and_does_not_wait_for_slot() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let f = Jobs::new(&format!("http://{}", listener.local_addr()?))?;
    let mut scheduler = ShadowScheduler::new();
    for _ in 0..5 {
        scheduler.shadow_workers.spawn(std::future::pending());
    }
    for n in 0..256 {
        scheduler
            .enqueue_shadow_task(256, input(&f, n, false))
            .map_err(|_| anyhow::anyhow!("pending slot"))?;
    }
    let (result, events) = super::b70_event_capture::capture(async {
        ExecutionCanaryRunner::new(f.config.clone()).admit_hot_observed_buy_quote(
            &f.base.store,
            &f.swap,
            Utc::now(),
            &mut scheduler,
        )
    })
    .await;
    result?;
    ensure!(events.len() == 1 && events[0]["reason"] == "hot_quote_capacity");
    ensure!(events[0]["signal_id"] == super::b58_fixture::signal_id("TokenA", "buy"));
    ensure!(events[0]["active"] == "5" && events[0]["pending"] == "256");
    ensure!(f.event()?.error.as_deref() == Some("hot_quote_capacity"));
    ensure!(f.counts()? == (1, 0));
    ensure!(scheduler.active_task_count() == 5 && scheduler.buffered_shadow_task_count() == 256);
    scheduler.hot_quotes.shutdown().await;
    scheduler.shadow_workers.abort_all();
    while let Some(result) = scheduler.shadow_workers.join_next().await {
        ensure!(result.is_err());
    }
    super::b64_http_fixture::no_more(&listener).await?;
    Ok(())
}
