//! COPY12's missed scheduler opportunity, through the actual LOCAL consumer and runners.
use crate::app_loop::ingress_execution_precheck::Scheduler;
use crate::app_tests::{controlled_copy_pipeline_fixture as frames, copy_cycle_fixture::Cycle};
use anyhow::{ensure, Context, Result};
use std::time::Duration;

fn count(c: &Cycle, table: &str) -> Result<i64> {
    ensure!([
        "ordered_source_sell_intents",
        "ordered_sell_quote_results",
        "rpc_owned_sell_handoffs",
        "rpc_owned_sell_dispatches"
    ]
    .contains(&table));
    Ok(c.case
        .sql()?
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
}
async fn ready_sell(c: &mut Cycle, duplicate: bool) -> Result<String> {
    let signature = c.event(true, 1, duplicate).await?;
    tokio::time::timeout(Duration::from_secs(2), async {
        while count(c, "ordered_source_sell_intents")? == 0 {
            c.poll().await?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("LOCAL SELL dependency continuation")??;
    Ok(signature)
}

async fn ready_cycle() -> Result<(Cycle, crate::execution_canary::ExecutionCanaryRunner)> {
    let mut c = Cycle::new().await?;
    c.case.source.tick().await?;
    c.event(true, 0, true).await?;
    let runner = c.runner()?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            c.check_peers()?;
            runner.process_tick(&c.case.bot, chrono::Utc::now()).await?;
            if c.case.ready()? {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("LOCAL BOT entry")??;
    c.event(false, 0, true).await?;
    c.case.source.tick().await?;
    ensure!(c.source.sends() == 2 && c.bot.sends() == 1);
    Ok((c, runner))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy12_committed_sell_wakes_real_runner_before_periodic_tick_and_closes_once() -> Result<()>
{
    let (mut c, runner) = ready_cycle().await?;
    let mut scheduler = Scheduler::new(&runner, Some(&c.consumer));
    scheduler.ready().await; // Initial periodic opportunity precedes SOURCE SELL.
    let sell = ready_sell(&mut c, true).await?;
    ensure!(count(&c, "ordered_source_sell_intents")? == 1);
    ensure!(count(&c, "ordered_sell_quote_results")? == 0 && c.bot.sends() == 1);
    // The old loop had only the15s interval. In the short remaining window,
    // the real consumer alone cannot fabricate a BOT order/quote or submit.
    tokio::time::sleep(Duration::from_millis(80)).await;
    ensure!(count(&c, "ordered_sell_quote_results")? == 0 && c.bot.sends() == 1);
    tokio::time::timeout(Duration::from_millis(200), scheduler.ready())
        .await
        .context("committed SELL failed to wake actual scheduler")?;
    runner.process_tick(&c.case.bot, chrono::Utc::now()).await?;
    // A single awakened tick starts the real guarded background continuation.
    // No tight process_tick loop supplies extra scheduling opportunities here.
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            c.check_peers()?;
            if c.case
                .bot
                .load_execution_canary_open_position(frames::USDC)?
                .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("LOCAL awakened SELL did not close BOT lot")??;
    ensure!(c.bot.sends() == 2);
    ensure!(c.count("orders")? == 2 && c.count("fills")? == 2);
    ensure!(
        count(&c, "rpc_owned_sell_handoffs")? == 1 && count(&c, "rpc_owned_sell_dispatches")? == 1
    );
    // Duplicate delivery and fresh periodic/restart reconciliation cannot rearm.
    ready_sell(&mut c, true).await?;
    runner.process_tick(&c.case.bot, chrono::Utc::now()).await?;
    ensure!(c.bot.sends() == 2 && c.source.sends() == 2);
    ensure!(c.count("orders")? == 2 && c.count("fills")? == 2);
    ensure!(
        c.case.sql()?.query_row(
            "SELECT count(*) FROM ordered_source_sell_intents WHERE signature=?1",
            [&sell],
            |r| r.get::<_, i64>(0)
        )? == 1
    );
    if let Ok(root) = std::env::var("COPY12_WAKE_EVIDENCE_DIR") {
        let out = std::path::Path::new(&root).join("LOCAL-awakened-cycle");
        std::fs::create_dir_all(&out)?;
        c.case.sql()?.execute(
            "VACUUM main INTO ?1",
            [out.join("bot.sqlite").to_string_lossy().as_ref()],
        )?;
        c.case.source.sql()?.execute(
            "VACUUM main INTO ?1",
            [out.join("source.sqlite").to_string_lossy().as_ref()],
        )?;
        std::fs::write(
            out.join("RESULT.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proof":"LOCAL_MODELED_PROVIDERS_ACTUAL_CONSUMER_RUNNERS",
                "source_sends":c.source.sends(),"bot_sends":c.bot.sends(),"bot_orders":c.count("orders")?,"bot_fills":c.count("fills")?,
                "own_bot_position":"CLOSED","execution_ticks_after_committed_sell_before_close":1,"source_sell_signature":sell,
                "duplicate_rearmed":false,"live_or_profitability_proof":false
            }))?,
        )?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy12_committed_hint_cannot_bypass_stop() -> Result<()> {
    let (mut c, runner) = ready_cycle().await?;
    let mut scheduler = Scheduler::new(&runner, Some(&c.consumer));
    scheduler.ready().await;
    ready_sell(&mut c, false).await?;
    std::fs::write(&c.app.execution.canary_kill_switch_path, b"LOCAL STOP")?;
    tokio::time::timeout(Duration::from_millis(200), scheduler.ready()).await?;
    let summary = runner.process_tick(&c.case.bot, chrono::Utc::now()).await?;
    ensure!(summary.skipped_reason == Some("kill_switch_active"));
    tokio::time::sleep(Duration::from_millis(100)).await;
    ensure!(c.bot.sends() == 1 && count(&c, "ordered_sell_quote_results")? == 0);
    ensure!(c
        .case
        .bot
        .load_execution_canary_open_position(frames::USDC)?
        .is_some());
    Ok(())
}
