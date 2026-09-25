//! Opt-in runtime bridge. One receive/association task, bounded charged channel,
//! no SQLite, signature eviction claims, legacy output or task-per-event spawning.
mod replay;
pub use replay::ReplayInput;
mod bridge;
mod convert;
mod parent;
pub(in crate::source) mod queue;
mod transport;
use anyhow::{Context, Result};
use copybot_config::IngestionConfig;
pub use queue::DeliveryEnvelope;
use tokio::{sync::mpsc, task::JoinHandle};
use std::collections::HashSet;

pub struct DeliveryReceiver {
    rx: mpsc::Receiver<DeliveryEnvelope>,
    task: Option<JoinHandle<Result<()>>>,
    wallet_scope: Option<HashSet<String>>,
}
impl DeliveryReceiver {
    pub fn start(config: &IngestionConfig, session: String, wallet_scope: Option<HashSet<String>>) -> Result<Self> {
        copybot_config::validate_delivery_source(config)?;
        let limits = config
            .yellowstone_association
            .as_ref()
            .context("association limits")?;
        let mut runtime = (*super::YellowstoneGrpcSource::new(config)?.runtime_config).clone();
        runtime.admission_wallets = wallet_scope.clone();
        let runtime = std::sync::Arc::new(runtime);
        let (tx, rx) = queue::channel(limits.queue.count, limits.queue.bytes);
        let limits = limits.clone();
        let task = tokio::spawn(async move { transport::run(runtime, limits, session, tx).await });
        Ok(Self {
            rx,
            task: Some(task),
            wallet_scope,
        })
    }
    pub fn wallet_scope(&self) -> Option<&HashSet<String>> { self.wallet_scope.as_ref() }
    pub fn stop(&mut self) {
        self.rx.close();
        if let Some(task) = self.task.take() {
            task.abort();
        }
        while self.rx.try_recv().is_ok() {}
    }
    pub async fn next(&mut self) -> Result<Option<DeliveryEnvelope>> {
        if let Some(v) = self.rx.recv().await {
            return Ok(Some(v));
        }
        if let Some(t) = self.task.as_mut() {
            t.await.context("delivery runtime task failed")??;
            self.task = None;
        }
        Ok(None)
    }
}
impl Drop for DeliveryReceiver {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}
