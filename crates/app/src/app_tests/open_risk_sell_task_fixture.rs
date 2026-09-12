use anyhow::{bail, Context, Result};
use std::time::Duration;
use tokio::task::JoinHandle;

// Only the owned-SELL fixture family uses this task boundary.
pub(super) struct RpcTask(Option<JoinHandle<Result<()>>>);

impl RpcTask {
    pub fn new(handle: JoinHandle<Result<()>>) -> Self {
        Self(Some(handle))
    }

    // Single use, even after an error. Cancellation is requested here and awaited;
    // a prior panic or Result error must never be mistaken for normal cancellation.
    pub async fn finish(&mut self) -> Result<()> {
        let handle = self.0.as_mut().context("RPC fixture already finished")?;
        handle.abort();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .context("RPC fixture shutdown timed out")?;
        self.0.take();
        match result {
            Ok(result) => result.context("RPC fixture server failed"),
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => bail!("RPC fixture task failed: {error}"),
        }
    }

    pub fn abort(&self) {
        if let Some(handle) = &self.0 {
            handle.abort();
        }
    }
}

impl Drop for RpcTask {
    fn drop(&mut self) {
        self.abort();
    }
}
