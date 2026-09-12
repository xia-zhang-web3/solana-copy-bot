#![cfg(test)]
use crate::app_tests::{b64_http_fixture as http, b70_job_fixture::Jobs};
use crate::shadow_scheduler::{ShadowScheduler, ShadowTaskInput, ShadowTaskKey};
use anyhow::{ensure, Result};
use copybot_shadow::{FollowSnapshot, ShadowService};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn b70_r1_reset_ready_sell_then_quote_fifo_and_same_key_buy() -> Result<()> {
    let _serial = crate::app_tests::b70_hooks::acquire().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let f = Jobs::new(&format!("http://{}", listener.local_addr()?))?;
    let mut s = ShadowScheduler::new();
    for _ in 0..2 {
        s.shadow_workers.spawn(std::future::pending());
    }
    let mut sockets = Vec::new();
    for n in 0..3 {
        s.admit_hot_quote(f.another(n)?)
            .map_err(|_| anyhow::anyhow!("admission"))?;
        sockets.extend(http::pair(&listener).await?);
    }
    let first = f.another(10)?;
    let first_id = first.origin.signal_id();
    let second = f.another(11)?;
    let second_id = second.origin.signal_id();
    s.admit_hot_quote(first)
        .map_err(|_| anyhow::anyhow!("first pending"))?;
    s.admit_hot_quote(second)
        .map_err(|_| anyhow::anyhow!("second pending"))?;
    s.shadow_workers.abort_all();
    while let Some(joined) = s.shadow_workers.join_next().await {
        ensure!(joined.err().is_some_and(|e| e.is_cancelled()));
    }
    let mut sell = f.swap.clone();
    sell.signature = "b70-r1-ready-sell".into();
    std::mem::swap(&mut sell.token_in, &mut sell.token_out);
    std::mem::swap(&mut sell.amount_in, &mut sell.amount_out);
    sell.exact_amounts = None;
    let key = ShadowTaskKey {
        wallet: f.swap.wallet.clone(),
        token: f.swap.token_out.clone(),
    };
    for swap in [sell, f.swap.clone()] {
        s.enqueue_shadow_task(
            256,
            ShadowTaskInput {
                swap,
                key: key.clone(),
                follow_snapshot: Arc::new(FollowSnapshot::default()),
            },
        )
        .map_err(|_| anyhow::anyhow!("shadow admission"))?;
    }
    s.shadow_scheduler_needs_reset = true;
    let shadow = ShadowService::new(Default::default());
    let prepare = |s: &mut ShadowScheduler| -> Result<()> {
        crate::prepare_shadow_scheduler_before_select(
            f.store(),
            &f.path().to_string_lossy(),
            &shadow,
            s,
            &Default::default(),
            &mut Default::default(),
            &mut Default::default(),
            &mut Default::default(),
        )?;
        Ok(())
    };
    prepare(&mut s)?;
    ensure!(
        !s.shadow_scheduler_needs_reset
            && s.active_task_count() == 5
            && s.shadow_workers.len() == 1
            && s.pending_shadow_task_count == 1
    );
    ensure!(s
        .hot_quotes
        .origins
        .values()
        .any(|o| o.signal_id() == first_id));
    ensure!(s.hot_quotes.pending.front().unwrap().origin.signal_id() == second_id);
    sockets.extend(http::pair(&listener).await?);
    let output = tokio::time::timeout(Duration::from_secs(2), s.shadow_workers.join_next())
        .await?
        .unwrap()?;
    ensure!(
        output.signature == "b70-r1-ready-sell",
        "per-key front only"
    );
    output.outcome?;
    s.mark_task_complete(&key);
    prepare(&mut s)?;
    ensure!(
        s.active_task_count() == 5
            && s.hot_quotes.pending() == 0
            && s.pending_shadow_task_count == 1
    );
    sockets.extend(http::pair(&listener).await?);
    for socket in sockets {
        let body = socket.quote();
        socket.reply(200, body).await?;
    }
    while s.hot_quotes.active() > 0 {
        let completion = tokio::time::timeout(Duration::from_secs(2), s.hot_quotes.finish_next())
            .await?
            .unwrap();
        ensure!(completion.output.is_ok());
    }
    prepare(&mut s)?;
    ensure!(s.shadow_workers.len() == 1 && s.pending_shadow_task_count == 0);
    let output = tokio::time::timeout(Duration::from_secs(2), s.shadow_workers.join_next())
        .await?
        .unwrap()?;
    ensure!(output.signature == f.swap.signature);
    output.outcome?;
    s.mark_task_complete(&key);
    s.hot_quotes.shutdown().await;
    http::no_more(&listener).await?;
    ensure!(s.active_task_count() == 0 && s.buffered_shadow_task_count() == 0);
    Ok(())
}

#[tokio::test]
async fn b70_r1_collected_completion_is_bounded_and_shutdown_releases_claim() -> Result<()> {
    let _serial = crate::app_tests::b70_hooks::acquire().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let f = Jobs::new(&format!("http://{}", listener.local_addr()?))?;
    let mut s = ShadowScheduler::new();
    let job = f.admission()?;
    let id = job.origin.signal_id();
    s.admit_hot_quote(job)
        .map_err(|_| anyhow::anyhow!("capacity"))?;
    for socket in http::pair(&listener).await? {
        let body = socket.quote();
        socket.reply(200, body).await?;
    }
    tokio::time::timeout(Duration::from_secs(2), s.hot_quotes.collect_next()).await?;
    ensure!(
        s.active_task_count() == 1
            && s.buffered_shadow_task_count() == 0
            && !s.hot_quotes.can_collect()
            && f.runner.entry_pending(&id)
    );
    s.hot_quotes.shutdown().await;
    ensure!(
        s.active_task_count() == 0
            && s.buffered_shadow_task_count() == 0
            && !f.runner.entry_pending(&id)
    );
    http::no_more(&listener).await?;
    Ok(())
}
