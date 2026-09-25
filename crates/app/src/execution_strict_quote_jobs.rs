//! Bounded strict quote jobs; the explicit policy may continue to unsigned ownership.
use super::{ExecutionQuoteCanaryRunner, ExecutionQuoteCanaryTickSummary};
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use copybot_config::{ExecutionConfig, IngestionConfig};
use copybot_storage_core::{association_inbox::InboxLimits, ordered_sell_quote::*, SqliteStore};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::task::JoinHandle;
#[path = "execution_strict_quote_http.rs"]
mod http;
const JOBS: usize = 4;
#[derive(Debug)]
struct Job {
    handle: JoinHandle<Result<Step>>,
    live: crate::execution_owned_sell_prepare::submit::guard::Live,
}
impl Drop for Job {
    fn drop(&mut self) {
        self.live
            .0
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.handle.abort();
    }
}
#[derive(Debug)]
enum Step {
    Completed,
    Skipped,
    CapacityRefused(QuoteCapacityRefusal),
}
#[derive(Debug)]
struct Pool {
    jobs: Vec<Job>,
}
impl Drop for Pool {
    fn drop(&mut self) {
        for job in &self.jobs {
            job.live.0.store(false, std::sync::atomic::Ordering::SeqCst);
            job.handle.abort();
        }
    }
}
#[derive(Debug, Clone)]
pub(crate) struct StrictQuotes {
    path: PathBuf,
    limits: InboxLimits,
    pool: Arc<Mutex<Pool>>,
}
impl StrictQuotes {
    pub(crate) fn for_ingestion(
        config: &ExecutionConfig,
        ingestion: &IngestionConfig,
        path: &str,
    ) -> Result<Option<Self>> {
        copybot_config::validate_owned_sell_preparation(config, ingestion)?;
        if config.owned_sell_preparation.is_some() {
            crate::execution_owned_sell_rpc::endpoint(config)?;
        }
        if ingestion.yellowstone_delivery_mode != "durable_association_v1" {
            return Ok(None);
        }
        ensure!(
            copybot_config::owned_sell_flags(config),
            "strict quotes require both trading flags=false"
        );
        let l = ingestion
            .yellowstone_association
            .as_ref()
            .context("strict quote inbox limits missing")?;
        Ok(Some(Self {
            path: path.into(),
            limits: InboxLimits {
                count: l.inbox.count,
                bytes: l.inbox.bytes,
                busy_ms: l.sqlite_busy_ms,
            },
            pool: Arc::new(Mutex::new(Pool { jobs: vec![] })),
        }))
    }
}
impl ExecutionQuoteCanaryRunner {
    pub(crate) fn with_strict_quotes(mut self, config: Option<StrictQuotes>) -> Self {
        self.strict = config;
        self
    }
    pub(super) fn strict_tick(&self) -> Result<ExecutionQuoteCanaryTickSummary> {
        let strict = self.strict.as_ref().context("strict mode missing")?;
        ensure!(
            copybot_config::owned_sell_flags(&self.config),
            "strict quotes trading flags changed"
        );
        let mut pool = strict
            .pool
            .lock()
            .map_err(|_| anyhow::anyhow!("strict quote pool poisoned"))?;
        let mut summary = ExecutionQuoteCanaryTickSummary::default();
        // Join only finished handles, without waiting on held network jobs in the app loop.
        let mut remaining = vec![];
        for mut job in pool.jobs.drain(..) {
            if !job.handle.is_finished() {
                remaining.push(job);
                continue;
            }
            use std::future::Future;
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            match std::pin::Pin::new(&mut job.handle).poll(&mut cx) {
                std::task::Poll::Ready(Ok(Ok(Step::Completed))) => summary.strict_completed += 1,
                std::task::Poll::Ready(Ok(Ok(Step::Skipped))) => {}
                std::task::Poll::Ready(Ok(Ok(Step::CapacityRefused(refusal)))) => {
                    summary.strict_capacity_refused += 1;
                    summary.last_error =
                        Some(format!("strict quote capacity refused: {refusal:?}"));
                }
                std::task::Poll::Ready(Ok(Err(e))) => {
                    summary.strict_errors += 1;
                    summary.last_error = Some(format!("strict quote storage/job failure: {e:#}"));
                }
                std::task::Poll::Ready(Err(e)) => {
                    summary.strict_errors += 1;
                    summary.last_error = Some(format!("strict quote task failure: {e}"));
                }
                std::task::Poll::Pending => remaining.push(job),
            }
        }
        pool.jobs = remaining;
        if !self.is_enabled()
            || (crate::execution_technical_cohort::active(&self.config)
                && crate::execution_technical_cohort::before_deadline(&self.config).is_err())
        {
            return Ok(summary);
        }
        // A technical cohort has at most one source SELL. Keep its quote and
        // preparation in one job until ownership is reserved (or the job ends):
        // a second claim must not replace the quote record while the first
        // job is still performing its guarded preparation.
        let slots =
            if crate::execution_technical_cohort::active(&self.config) && !pool.jobs.is_empty() {
                0
            } else {
                (JOBS - pool.jobs.len()).min(self.config.canary_batch_limit.max(1) as usize)
            };
        for _ in 0..slots {
            let path = strict.path.clone();
            let limits = strict.limits;
            let config = self.config.clone();
            let client = self.http.clone();
            let live = crate::execution_owned_sell_prepare::submit::guard::Live(
                Arc::new(std::sync::atomic::AtomicBool::new(true)),
                Arc::new(std::sync::atomic::AtomicI64::new(-1)),
            );
            let owner = live.clone();
            let handle = tokio::spawn(async move {
                let endpoint = reqwest::Url::parse(&config.quote_canary_base_url)?;
                ensure!(
                    endpoint.username().is_empty()
                        && endpoint.password().is_none()
                        && endpoint.query().is_none()
                        && endpoint.fragment().is_none(),
                    "strict quote endpoint must not contain credentials/query/fragment"
                );
                let endpoint = endpoint.to_string();
                let preparation = config.owned_sell_preparation.is_some();
                let prepared = tokio::task::spawn_blocking(move || -> Result<_> {
                    let store = SqliteStore::open(path)?;
                    store.set_busy_timeout(std::time::Duration::from_millis(limits.busy_ms))?;
                    let claim = if preparation {
                        store.claim_strict_sell_quote_for_owned_preparation(
                            limits,
                            &endpoint,
                            Utc::now,
                        )?
                    } else {
                        store.claim_strict_sell_quote(limits, &endpoint, Utc::now)?
                    };
                    Ok((store, claim))
                })
                .await??;
                let (mut store, step) = prepared;
                let claim = match step {
                    QuoteClaimStep::Claimed(claim) => claim,
                    QuoteClaimStep::CapacityRefused(refusal) => {
                        return Ok(Step::CapacityRefused(refusal))
                    }
                    _ => return Ok(Step::Skipped),
                };
                let claim = if crate::execution_owned_sell_rpc::fractional::enabled(&config) {
                    crate::execution_owned_sell_rpc::fractional::collect(
                        &client, &mut store, &config, claim, limits,
                    )
                    .await?
                } else {
                    ensure!(
                        claim.binding.fractional.is_none(),
                        "fraction_contract_not_enabled"
                    );
                    claim
                };
                let binding = claim.binding.clone();
                let mut recheck = || {
                    // Mutable capture keeps the connection owned by this Send job.
                    let store = &mut store;
                    store.recheck_strict_sell_quote(&claim, limits, Utc::now())
                };
                let (observation, body) = if preparation {
                    http::fetch_with_body(&client, &config, &binding, &mut recheck).await
                } else {
                    (
                        http::fetch(&client, &config, &binding, &mut recheck).await,
                        None,
                    )
                };
                let (mut store, observation) = tokio::task::spawn_blocking(move || -> Result<_> {
                    let observation =
                        store.complete_strict_sell_quote(&claim, limits, observation, Utc::now)?;
                    Ok((store, observation))
                })
                .await??;
                if config.owned_sell_preparation.is_some()
                    && observation.outcome == QuoteOutcome::Current
                {
                    let runtime = tokio::runtime::Handle::current();
                    tokio::task::spawn_blocking(move || {
                        runtime.block_on(crate::execution_owned_sell_prepare::run(
                            &mut store,
                            &config,
                            limits,
                            observation,
                            body.context("owned_sell_quote_body_missing")?,
                            owner,
                        ))
                    })
                    .await??;
                }
                Ok(Step::Completed)
            });
            pool.jobs.push(Job { handle, live });
        }
        summary.strict_running = pool.jobs.len();
        Ok(summary)
    }
}
