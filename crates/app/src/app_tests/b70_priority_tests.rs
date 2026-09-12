use super::{b64_http_fixture as http, b70_job_fixture::Jobs};
use crate::execution_priority_fee::{tagged_fee, PriorityFee};
use crate::execution_quote_canary::{job::HotQuoteOutput, ExecutionQuoteCanaryRunner};
use crate::shadow_scheduler::ShadowScheduler;
use anyhow::{ensure, Result};
use chrono::Utc;
use serde_json::json;
use tokio::net::TcpListener;

async fn seed(f: &Jobs, listener: TcpListener) -> Result<()> {
    let admission = f.admission()?;
    let (output, server) = tokio::join!(
        admission.job.run(),
        http::serve(listener, Default::default())
    );
    server?;
    ensure!(
        f.runner.finish_hot_quote(
            &f.base.store,
            &admission.origin,
            Ok(output),
            Utc::now(),
            None
        )? == "hot_quote_recorded"
    );
    Ok(())
}

#[tokio::test]
async fn b70_existing_priority_job_shares_throttle_cache_and_preserves_zero_unknown() -> Result<()>
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut f = Jobs::new(&format!("http://{}", listener.local_addr()?))?;
    seed(&f, listener).await?;
    let original = f.event()?;
    ensure!(
        original.priority_fee_lamports.is_none()
            && tagged_fee(original.priority_fee_json.as_deref()).is_err()
    );
    let rpc = TcpListener::bind("127.0.0.1:0").await?;
    f.config.priority_fee_canary_enabled = true;
    f.config.priority_fee_canary_rpc_url = format!("http://{}", rpc.local_addr()?);
    f.config.priority_fee_canary_timeout_ms = 2_000;
    f.config.priority_fee_canary_cache_ttl_ms = 60_000;
    f.config.priority_fee_canary_min_request_interval_ms = 60_000;
    // One sampler per runner; jobs clone its actual shared state.
    f.runner = ExecutionQuoteCanaryRunner::new(f.config.clone());
    let mut scheduler = ShadowScheduler::new();
    scheduler
        .admit_hot_quote(f.admission()?)
        .map_err(|_| anyhow::anyhow!("admission"))?;
    let held = http::accept(&rpc).await?;
    ensure!(held.body["method"] == "qn_estimatePriorityFees");
    ensure!(f
        .runner
        .prepare_hot_quote(&f.base.store, &f.swap, Utc::now())?
        .is_none());
    // A periodic entry retry would issue HTTP without the same pending-ID guard.
    let tick = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        f.runner.process_tick(
            &f.base.store,
            "shadow_recorded",
            Utc::now(),
            f.swap.ts_utc,
            8,
        ),
    )
    .await??;
    ensure!(tick.entry_candidates == 0 && f.event()? == original);

    let observed_b = f.another(90)?;
    let swap_b = observed_b.origin.swap.clone();
    let mut event_b = original.clone();
    event_b.event_id = observed_b.origin.event_id();
    event_b.signal_id = Some(observed_b.origin.signal_id());
    drop(observed_b);
    f.base.store.record_execution_quote_canary_event(&event_b)?;
    let b = f
        .runner
        .prepare_hot_quote(&f.base.store, &swap_b, Utc::now())?
        .unwrap();
    let output_b = tokio::time::timeout(std::time::Duration::from_millis(100), b.job.run()).await?;
    let HotQuoteOutput::ExistingPriority(ref sample) = output_b else {
        unreachable!()
    };
    ensure!(sample.as_ref().unwrap().status == "skipped");
    ensure!(
        f.runner
            .finish_hot_quote(&f.base.store, &b.origin, Ok(output_b), Utc::now(), None)?
            == "hot_quote_priority_completed"
    );
    drop(b.origin);
    ensure!(
        f.base
            .store
            .load_execution_quote_canary_event_by_id(&event_b.event_id)?
            == Some(event_b.clone())
    );
    http::no_more(&rpc).await?;
    held.reply(200, json!({"jsonrpc":"2.0","result":{"recommended":0}}))
        .await?;
    let a = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        scheduler.hot_quotes.finish_next(),
    )
    .await?
    .unwrap();
    ensure!(
        f.runner
            .finish_hot_quote(&f.base.store, &a.origin, a.output, Utc::now(), None)?
            == "hot_quote_priority_completed"
    );
    drop(a.origin);
    let updated = f.event()?;
    ensure!(updated.priority_fee_lamports.is_none());
    ensure!(
        tagged_fee(updated.priority_fee_json.as_deref())?
            == PriorityFee::MicroLamportsPerComputeUnit(0)
    );
    ensure!(updated.request_ts == original.request_ts && updated.signal_ts == original.signal_ts);
    ensure!(updated.quote_response_json == original.quote_response_json);
    let b = f
        .runner
        .prepare_hot_quote(&f.base.store, &swap_b, Utc::now())?
        .unwrap();
    let output_b = b.job.run().await;
    f.runner
        .finish_hot_quote(&f.base.store, &b.origin, Ok(output_b), Utc::now(), None)?;
    drop(b.origin);
    let cached = f
        .base
        .store
        .load_execution_quote_canary_event_by_id(&event_b.event_id)?
        .unwrap();
    ensure!(
        tagged_fee(cached.priority_fee_json.as_deref())?
            == PriorityFee::MicroLamportsPerComputeUnit(0)
    );
    ensure!(
        f.counts()? == (2, 2),
        "priority jobs cannot append quote provider samples"
    );
    http::no_more(&rpc).await?;
    scheduler.hot_quotes.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn b70_existing_priority_late_winner_and_stale_completion_preserve_original() -> Result<()> {
    for changed in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut f = Jobs::new(&format!("http://{}", listener.local_addr()?))?;
        seed(&f, listener).await?;
        let rpc = TcpListener::bind("127.0.0.1:0").await?;
        f.config.priority_fee_canary_enabled = true;
        f.config.priority_fee_canary_rpc_url = format!("http://{}", rpc.local_addr()?);
        f.runner = ExecutionQuoteCanaryRunner::new(f.config.clone());
        let job = f.admission()?;
        let (output, server) = tokio::join!(job.job.run(), async {
            let request = http::accept(&rpc).await?;
            ensure!(request.body["method"] == "qn_estimatePriorityFees");
            request
                .reply(200, json!({"result":{"recommended":0}}))
                .await
        });
        server?;
        let now = if changed {
            rusqlite::Connection::open(&f.base.path)?.execute(
                "UPDATE execution_quote_canary_events SET error='another owner' WHERE event_id=?1",
                [&job.origin.event_id()],
            )?;
            Utc::now()
        } else {
            f.swap.ts_utc + chrono::Duration::seconds(31)
        };
        let before = f.event()?;
        ensure!(
            f.runner
                .finish_hot_quote(&f.base.store, &job.origin, Ok(output), now, None)?
                == if changed {
                    "hot_quote_entry_changed"
                } else {
                    "hot_quote_stale"
                }
        );
        drop(job.origin);
        ensure!(f.event()? == before && f.counts()? == (1, 2));
        http::no_more(&rpc).await?;
    }
    Ok(())
}
