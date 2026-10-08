//! Independently polled fetches; permits cover network, raw queue and application.
use anyhow::{ensure, Context, Result};
use futures_util::{stream, StreamExt};
use std::{collections::BTreeMap, future::Future, sync::Arc};
use tokio::{
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};

pub(crate) struct Charged<T> {
    value: T,
    permit: OwnedSemaphorePermit,
}
impl<T> Charged<T> {
    pub(crate) fn from_parts(value: T, permit: OwnedSemaphorePermit) -> Self {
        Self { value, permit }
    }
    pub(crate) fn map<R>(self, map: impl FnOnce(T) -> R) -> Charged<R> {
        let (value, permit) = self.into_parts();
        Charged::from_parts(map(value), permit)
    }
    /// Keep the permit until normalization and ordered application both finish.
    pub(crate) fn into_parts(self) -> (T, OwnedSemaphorePermit) {
        (self.value, self.permit)
    }
}

#[derive(Clone)]
pub(crate) struct PipelineWindow {
    bound: usize,
    permits: Arc<Semaphore>,
}
impl PipelineWindow {
    pub(crate) fn new(bound: usize) -> Result<Self> {
        ensure!(bound > 0, "http_recovery_pipeline_bounds");
        Ok(Self {
            bound,
            permits: Arc::new(Semaphore::new(bound)),
        })
    }
    pub(crate) fn bound(&self) -> usize {
        self.bound
    }
    pub(crate) async fn acquire(&self) -> Result<OwnedSemaphorePermit> {
        self.permits
            .clone()
            .acquire_owned()
            .await
            .context("http_recovery_pipeline_permit_closed")
    }
}

pub(crate) struct OrderedPipeline<T> {
    rx: mpsc::Receiver<Charged<Result<T>>>,
    task: Option<JoinHandle<Result<()>>>,
}
impl<T: Send + 'static> OrderedPipeline<T> {
    pub(crate) fn start<F, Fut>(
        slots: Vec<u64>,
        width: usize,
        window: usize,
        fetch: F,
    ) -> Result<Self>
    where
        F: FnMut(u64) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        Self::start_in_window(slots, width, PipelineWindow::new(window)?, fetch)
    }
    pub(crate) fn start_in_window<F, Fut>(
        slots: Vec<u64>,
        width: usize,
        window: PipelineWindow,
        mut fetch: F,
    ) -> Result<Self>
    where
        F: FnMut(u64) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        ensure!(
            width > 0 && window.bound >= width,
            "http_recovery_pipeline_bounds"
        );
        let permits = window.permits;
        let (tx, rx) = mpsc::channel(window.bound);
        let task = tokio::spawn(async move {
            let pending = stream::iter(slots.into_iter().enumerate())
                .map(move |(ordinal, slot)| {
                    let permits = permits.clone();
                    let future = fetch(slot);
                    async move {
                        let permit = permits
                            .acquire_owned()
                            .await
                            .context("http_recovery_pipeline_permit_closed")?;
                        let value = future.await;
                        Ok::<_, anyhow::Error>((ordinal, Charged { value, permit }))
                    }
                })
                .buffer_unordered(width);
            tokio::pin!(pending);
            let mut ordered = BTreeMap::new();
            let mut next = 0;
            while let Some(fetched) = pending.next().await {
                let (ordinal, fetched) = fetched?;
                ordered.insert(ordinal, fetched);
                while let Some(ready) = ordered.remove(&next) {
                    let failed = ready.value.is_err();
                    if tx.send(ready).await.is_err() || failed {
                        return Ok(());
                    }
                    next += 1;
                }
            }
            Ok(())
        });
        Ok(Self {
            rx,
            task: Some(task),
        })
    }
    pub(crate) async fn next(&mut self) -> Result<Option<Charged<T>>> {
        if let Some(value) = self.rx.recv().await {
            let (value, permit) = value.into_parts();
            return value.map(|value| Some(Charged { value, permit }));
        }
        if let Some(task) = self.task.take() {
            task.await.context("http_recovery_pipeline_task_failed")??;
        }
        Ok(None)
    }
}
impl<T> Drop for OrderedPipeline<T> {
    fn drop(&mut self) {
        // STOP, normalization/apply failure and owner cancellation all drop
        // this handle; no detached producer may continue broker requests.
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
#[path = "../../source_tests/http_ordered_pipeline_tests.rs"]
mod pipeline_tests;

#[cfg(test)]
#[path = "../../source_tests/http_ordered_pipeline_profile_tests.rs"]
mod profile_tests;
