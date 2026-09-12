#![cfg(test)]
use super::*;
use crate::app_tests::b70_job_fixture::Jobs;
use anyhow::{ensure, Result};

#[tokio::test]
async fn b70_panicked_network_task_uses_real_join_identity_and_releases_claim() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let f = Jobs::new(&format!("http://{}", listener.local_addr()?))?;
    let job = f.admission()?;
    let signal = job.origin.signal_id();
    let mut jobs = HotQuotes::default();
    let handle = jobs
        .tasks
        .spawn(async { panic!("bounded injected network panic") });
    jobs.origins.insert(handle.id(), job.origin);
    let completion = tokio::time::timeout(std::time::Duration::from_secs(1), jobs.finish_next())
        .await?
        .unwrap();
    ensure!(completion.output.err() == Some("hot_quote_panicked"));
    ensure!(completion.origin.signal_id() == signal && f.runner.entry_pending(&signal));
    drop(completion.origin);
    ensure!(!f.runner.entry_pending(&signal) && jobs.active() == 0);
    jobs.shutdown().await;
    Ok(())
}
