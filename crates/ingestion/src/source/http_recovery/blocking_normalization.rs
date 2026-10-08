//! One owning CPU lane; cancellation cannot release unfinished raw work.
use super::ordered_pipeline::{Charged, PipelineWindow};
use anyhow::{ensure, Context, Result};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};

#[derive(Clone)]
pub(crate) struct BlockingNormalization {
    lane: Arc<Semaphore>,
    window: Arc<Mutex<Option<PipelineWindow>>>,
}
impl Default for BlockingNormalization {
    fn default() -> Self {
        Self {
            lane: Arc::new(Semaphore::new(1)),
            window: Arc::new(Mutex::new(None)),
        }
    }
}
pub(crate) struct NormalizationOwner {
    _lane: OwnedSemaphorePermit,
}
pub(crate) struct Transformation<T> {
    pub result: Result<T>,
    /// Closure wall time, including CPU descheduling; not OS on-CPU time.
    pub execution: Duration,
    /// Blocking scheduling/join wait, excluding closure execution.
    pub waiting: Duration,
}
struct OwnedJob<T>(JoinHandle<T>);
impl<T> Drop for OwnedJob<T> {
    fn drop(&mut self) {
        // Tokio can abort a queued blocking job. Started pure CPU work finishes
        // with its lane/raw permits, then drops its unobserved result.
        self.0.abort();
    }
}
impl BlockingNormalization {
    pub(crate) async fn begin(&self, bound: usize) -> Result<(NormalizationOwner, PipelineWindow)> {
        // Acquire BEFORE a replacement producer starts any request. The same
        // whole window also charges residual work from the previous generation.
        let lane = self
            .lane
            .clone()
            .acquire_owned()
            .await
            .context("http_recovery_normalization_lane_closed")?;
        let mut window = self
            .window
            .lock()
            .map_err(|_| anyhow::anyhow!("http_recovery_normalization_window_poisoned"))?;
        if let Some(existing) = window.as_ref() {
            ensure!(
                existing.bound() == bound,
                "http_recovery_normalization_window_changed"
            );
        } else {
            *window = Some(PipelineWindow::new(bound)?);
        }
        Ok((
            NormalizationOwner { _lane: lane },
            window.as_ref().expect("window").clone(),
        ))
    }
}
impl NormalizationOwner {
    pub(crate) async fn run<T, R, F>(
        self,
        input: Charged<T>,
        transform: F,
    ) -> Result<(Charged<Transformation<R>>, Self)>
    where
        T: Send + 'static,
        R: Send + 'static,
        F: FnOnce(T) -> Result<R> + Send + 'static,
    {
        let queued = Instant::now();
        let mut job = OwnedJob(tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let transformed = input.map(transform);
            let execution = started.elapsed();
            let (result, permit) = transformed.into_parts();
            (result, execution, permit, self)
        }));
        let (result, execution, permit, owner) = (&mut job.0)
            .await
            .context("http_recovery_normalization_task_failed")?;
        let waiting = queued.elapsed().saturating_sub(execution);
        Ok((
            Charged::from_parts(
                Transformation {
                    result,
                    execution,
                    waiting,
                },
                permit,
            ),
            owner,
        ))
    }
}

#[cfg(test)]
#[path = "../../source_tests/http_blocking_normalization_tests.rs"]
mod normalization_tests;
