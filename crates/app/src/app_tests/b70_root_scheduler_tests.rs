use super::{b64_http_fixture as http, b70_job_fixture::Jobs, *};
use anyhow::{ensure, Result};
use std::{sync::Arc, time::Duration};

async fn arm(extra_pending: usize) -> Result<(usize, usize, usize, usize)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let f = Jobs::new(&format!("http://{}", listener.local_addr()?))?;
    let mut scheduler = ShadowScheduler::new();
    for _ in 0..2 {
        scheduler.shadow_workers.spawn(std::future::pending());
    }
    let mut held = Vec::new();
    for n in 0..3 {
        scheduler
            .admit_hot_quote(f.another(100 + n)?)
            .map_err(|_| anyhow::anyhow!("initial hot admission"))?;
        held.extend(http::pair(&listener).await?);
    }
    ensure!(scheduler.active_task_count() == 5);
    for n in 0..extra_pending {
        scheduler
            .admit_hot_quote(f.another(200 + n)?)
            .map_err(|_| anyhow::anyhow!("pending hot admission"))?;
    }
    ensure!(scheduler.hot_quotes.pending() == extra_pending);
    scheduler.shadow_workers.abort_all();
    while let Some(result) = scheduler.shadow_workers.join_next().await {
        ensure!(result.err().is_some_and(|e| e.is_cancelled()));
    }
    ensure!(scheduler.active_task_count() == 3);
    let mut swap = f.swap.clone();
    swap.signature = "b70-root-ready-sell".into();
    std::mem::swap(&mut swap.token_in, &mut swap.token_out);
    std::mem::swap(&mut swap.amount_in, &mut swap.amount_out);
    if let Some(exact) = swap.exact_amounts.as_mut() {
        std::mem::swap(&mut exact.amount_in_raw, &mut exact.amount_out_raw);
        std::mem::swap(
            &mut exact.amount_in_decimals,
            &mut exact.amount_out_decimals,
        );
    }
    let key = crate::swap_classification::shadow_task_key_for_swap(
        &swap,
        crate::shadow_scheduler::ShadowSwapSide::Sell,
    );
    let sell = crate::shadow_scheduler::ShadowTaskInput {
        swap,
        key,
        follow_snapshot: Arc::new(FollowSnapshot::default()),
    };
    scheduler
        .enqueue_shadow_task(256, sell)
        .map_err(|_| anyhow::anyhow!("ready SELL admission"))?;
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
    let snapshot = (
        scheduler.active_task_count(),
        scheduler.hot_quotes.active(),
        scheduler.shadow_workers.len(),
        scheduler.pending_shadow_task_count,
    );
    scheduler.hot_quotes.shutdown().await;
    for request in held {
        request.cancelled().await?;
    }
    while !scheduler.shadow_workers.is_empty() {
        let output =
            tokio::time::timeout(Duration::from_secs(2), scheduler.shadow_workers.join_next())
                .await?
                .context("missing shadow completion")??;
        ensure!(output.signature == "b70-root-ready-sell");
        output.outcome?;
    }
    http::no_more(&listener).await?;
    ensure!(scheduler.active_task_count() == 0);
    Ok(snapshot)
}
#[tokio::test]
async fn b70_root_ready_sell_is_not_bypassed_by_pending_hot() -> Result<()> {
    let _serial = super::b70_hooks::acquire().await;
    let control = arm(0).await?;
    let with_pending_hot = arm(2).await?;
    println!("B70_ROOT_SCHEDULER control={control:?} pending_hot={with_pending_hot:?}");
    ensure!(control == (4, 3, 1, 0));
    ensure!(with_pending_hot.2 == 1 && with_pending_hot.3 == 0,
        "ready SELL bypassed: control={control:?}, pending_hot={with_pending_hot:?}; (active,hot,shadow,pending_shadow)");
    Ok(())
}
