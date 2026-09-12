use super::{b64_http_fixture as http, b70_job_fixture::Jobs};
use crate::execution_quote_canary::job::HotQuoteOutput;
use crate::shadow_scheduler::ShadowScheduler;
use anyhow::{ensure, Result};
use chrono::{Duration, Utc};
use tokio::net::TcpListener;

#[tokio::test]
async fn b70_completion_rechecks_stale_source_identity_gate_and_existing_winner() -> Result<()> {
    for case in [
        "healthy",
        "stale",
        "source",
        "observation",
        "raw",
        "gate",
        "existing",
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let f = Jobs::new(&format!("http://{}", listener.local_addr()?))?;
        let mut scheduler = ShadowScheduler::new();
        scheduler
            .admit_hot_quote(f.admission()?)
            .map_err(|_| anyhow::anyhow!("capacity"))?;
        let requests = http::pair(&listener).await?;
        for request in requests {
            let body = request.quote();
            request.reply(200, body).await?;
        }
        let completion = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            scheduler.hot_quotes.finish_next(),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing completion"))?;
        let signal_id = completion.origin.signal_id();
        ensure!(f.runner.entry_pending(&signal_id));
        let mut now = Utc::now();
        if case == "stale" {
            now = f.swap.ts_utc + Duration::seconds(31);
        }
        if case == "source" {
            f.base
                .store
                .deactivate_follow_wallet(&f.swap.wallet, now, "removed")?;
        }
        if case == "observation" {
            rusqlite::Connection::open(&f.base.path)?.execute(
                "UPDATE observed_swaps SET wallet_id='foreign' WHERE signature=?1",
                [&f.swap.signature],
            )?;
        }
        if case == "raw" {
            rusqlite::Connection::open(&f.base.path)?.execute(
                "UPDATE observed_swaps SET qty_out_raw='7' WHERE signature=?1",
                [&f.swap.signature],
            )?;
        }
        if case == "existing" {
            let HotQuoteOutput::Fresh(ref bundle) = completion.output.as_ref().unwrap() else {
                unreachable!()
            };
            let mut winner = bundle.event.clone();
            winner.wallet_id = "foreign-winner".into();
            f.base.store.record_execution_quote_canary_event(&winner)?;
        }
        let reason = f.runner.finish_hot_quote(
            &f.base.store,
            &completion.origin,
            completion.output,
            now,
            (case == "gate").then_some("hot_quote_operator_stop"),
        )?;
        drop(completion.origin);
        ensure!(!f.runner.entry_pending(&signal_id));
        ensure!(
            f.counts()? == if case == "healthy" { (1, 2) } else { (1, 0) },
            "{case}"
        );
        let event = f.event()?;
        ensure!(event.request_ts <= Utc::now());
        if case == "healthy" {
            ensure!(reason == "hot_quote_recorded" && event.quote_status == "ok");
            ensure!(f
                .runner
                .prepare_hot_quote(&f.base.store, &f.swap, Utc::now())?
                .is_none());
        } else if case == "existing" {
            ensure!(reason == "hot_quote_entry_changed" && event.wallet_id == "foreign-winner");
        } else {
            ensure!(
                event.quote_status == "error"
                    && event.decision_status.as_deref() == Some("unknown"),
                "{case}: {event:?}"
            );
            ensure!(event.error.as_deref() == Some(reason));
        }
        http::no_more(&listener).await?;
        scheduler.hot_quotes.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn b70_claim_survives_shadow_completion_cancel_and_restart_allows_retry() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let f = Jobs::new(&format!("http://{}", listener.local_addr()?))?;
    let mut scheduler = ShadowScheduler::new();
    let job = f.admission()?;
    let signal = job.origin.signal_id();
    scheduler
        .admit_hot_quote(job)
        .map_err(|_| anyhow::anyhow!("capacity"))?;
    let requests = http::pair(&listener).await?;
    scheduler.mark_task_complete(&crate::shadow_scheduler::ShadowTaskKey {
        wallet: f.swap.wallet.clone(),
        token: f.swap.token_out.clone(),
    });
    ensure!(f.runner.entry_pending(&signal));
    ensure!(f
        .runner
        .prepare_hot_quote(&f.base.store, &f.swap, Utc::now())?
        .is_none());
    let ((), sockets) = tokio::join!(scheduler.hot_quotes.shutdown(), async {
        for request in requests {
            request.cancelled().await?;
        }
        Ok::<_, anyhow::Error>(())
    });
    sockets?;
    ensure!(!f.runner.entry_pending(&signal) && scheduler.active_task_count() == 0);
    ensure!(f.counts()? == (0, 0));
    let reopened = copybot_storage_core::SqliteStore::open(&f.base.path)?;
    let runner = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(f.config.clone());
    let job = runner
        .prepare_hot_quote(&reopened, &f.swap, Utc::now())?
        .unwrap();
    let (output, replies) = tokio::join!(job.job.run(), http::serve(listener, Default::default()));
    replies?;
    ensure!(
        runner.finish_hot_quote(&reopened, &job.origin, Ok(output), Utc::now(), None)?
            == "hot_quote_recorded"
    );
    drop(job.origin);
    ensure!(f.counts()? == (1, 2));
    Ok(())
}
