//! One cancellable receipt batch per shared runner, with no network wait in its tick.
use anyhow::{Context, Result};
use copybot_config::ExecutionConfig;
use copybot_storage_core::SqliteStore;
use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};
use tokio::{sync::oneshot, task::JoinHandle};

#[derive(Debug, Default)]
pub(crate) struct Completed {
    pub checked: usize,
    pub reconciled: usize,
    pub pending_reason: Option<String>,
}
#[derive(Debug)]
struct Job {
    handle: JoinHandle<Result<Completed>>,
    cancel: Option<oneshot::Sender<()>>,
}
impl Drop for Job {
    fn drop(&mut self) {
        // abort also prevents a queued blocking job from starting. A running job
        // is stopped by select below, dropping its in-flight RPC future. No held
        // reservation is released; any already committed receipt remains durable.
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        self.handle.abort();
    }
}
#[derive(Debug, Clone)]
pub(crate) struct Recovery {
    path: PathBuf,
    job: Arc<Mutex<Option<Job>>>,
}
impl Recovery {
    pub(crate) fn new(path: &str) -> Self {
        Self {
            path: path.into(),
            job: Arc::new(Mutex::new(None)),
        }
    }
    pub(crate) fn tick(&self, config: &ExecutionConfig) -> Result<Option<Completed>> {
        let mut slot = self
            .job
            .lock()
            .map_err(|_| anyhow::anyhow!("owned recovery pool poisoned"))?;
        if let Some(job) = slot.as_mut() {
            // Poll once. Never join an unfinished job or keep a lock across I/O.
            let mut cx = std::task::Context::from_waker(Waker::noop());
            match Pin::new(&mut job.handle).poll(&mut cx) {
                Poll::Pending => return Ok(None),
                Poll::Ready(result) => {
                    slot.take();
                    return Ok(Some(
                        result
                            .context("owned recovery task failed")?
                            .context("owned recovery receipt batch failed")?,
                    ));
                }
            }
        }
        let path = self.path.clone();
        let config = config.clone();
        let (cancel, cancelled) = oneshot::channel();
        #[cfg(test)]
        crate::app_tests::b136_r1_hooks::scheduled(&path);
        let handle = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            let _running = crate::app_tests::b136_r1_hooks::Running::new(&path);
            // Own the I/O runtime as well as the SQLite connection: cancellation
            // can finish even as the parent runtime shuts down. No borrowed
            // foreground store or non-Send receipt future crosses task boundaries.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                tokio::select! {
                    biased;
                    _ = cancelled => anyhow::bail!("owned recovery runner dropped"),
                    result = async {
                        let store = SqliteStore::open(path)?;
                        super::recover(&store, &config).await
                    } => result,
                }
            })
        });
        *slot = Some(Job {
            handle,
            cancel: Some(cancel),
        });
        Ok(None)
    }
}
