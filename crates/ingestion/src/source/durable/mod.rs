//! Opt-in runtime bridge. One receive/association task, bounded charged channel,
//! no SQLite, signature eviction claims, legacy output or task-per-event spawning.
pub(in crate::source) mod recovery;
mod replay;
pub use recovery::replay_scope;
pub use replay::ReplayInput;
mod processing_telemetry;
mod telemetry;
pub use processing_telemetry::{HttpRecoverySnapshot, IngressProcessingSnapshot, TimingSnapshot};
pub use telemetry::{DurableIngressSnapshot, TransportClass, TransportStage};
mod bridge;
pub(in crate::source) mod capture_scope;
mod convert;
mod parent;
pub(in crate::source) mod queue;
mod transport;
#[cfg(test)]
pub(in crate::source) use transport::TestReader;
pub(in crate::source) mod transport_diagnostics;
use anyhow::{Context, Result};
use copybot_config::IngestionConfig;
pub use queue::DeliveryEnvelope;
use std::collections::HashSet;
use std::sync::Arc;
use telemetry::DurableIngressTelemetry;
use tokio::{sync::mpsc, task::JoinHandle};

pub struct DeliveryReceiver {
    rx: mpsc::Receiver<DeliveryEnvelope>,
    task: Option<JoinHandle<Result<()>>>,
    wallet_scope: Option<HashSet<String>>,
    bot_signer: Option<String>,
    telemetry: Arc<DurableIngressTelemetry>,
    recovery: Option<recovery::RecoveryCursor>,
}
impl DeliveryReceiver {
    pub fn start(
        config: &IngestionConfig,
        session: String,
        wallet_scope: Option<HashSet<String>>,
    ) -> Result<Self> {
        Self::start_labeled(config, session, wallet_scope, None)
    }
    pub fn start_labeled(
        config: &IngestionConfig,
        session: String,
        wallet_scope: Option<HashSet<String>>,
        bot_signer: Option<String>,
    ) -> Result<Self> {
        copybot_config::validate_delivery_source(config)?;
        if let Some(bot) = bot_signer.as_ref() {
            anyhow::ensure!(
                wallet_scope
                    .as_ref()
                    .is_some_and(|scope| scope.contains(bot)),
                "bot telemetry signer outside admission scope"
            );
        }
        let limits = config
            .yellowstone_association
            .as_ref()
            .context("association limits")?;
        let mut runtime = (*super::YellowstoneGrpcSource::new(config)?.runtime_config).clone();
        runtime.admission_wallets = wallet_scope.clone();
        let runtime = std::sync::Arc::new(runtime);
        let (tx, rx) = queue::channel(limits.queue.count, limits.queue.bytes);
        let limits = limits.clone();
        let telemetry = Arc::new(DurableIngressTelemetry::default());
        let report = Arc::clone(&telemetry);
        let task_bot = bot_signer.clone();
        let task = tokio::spawn(async move {
            transport::run(runtime, limits, session, tx, task_bot, report, None).await
        });
        Ok(Self {
            rx,
            task: Some(task),
            wallet_scope,
            bot_signer,
            telemetry,
            recovery: None,
        })
    }
    pub fn start_recovering_labeled(
        config: &IngestionConfig,
        session: String,
        wallet_scope: HashSet<String>,
        bot_signer: Option<String>,
        restored: Option<copybot_core_types::association_recovery::DurableCheckpoint>,
    ) -> Result<Self> {
        copybot_config::validate_delivery_source(config)?;
        let scope = replay_scope(config, &wallet_scope)?;
        if let Some(bot) = bot_signer.as_ref() {
            anyhow::ensure!(
                wallet_scope.contains(bot),
                "bot telemetry signer outside admission scope"
            );
        }
        let limits = config
            .yellowstone_association
            .clone()
            .context("association limits")?;
        let mut runtime = (*super::YellowstoneGrpcSource::new(config)?.runtime_config).clone();
        runtime.admission_wallets = Some(wallet_scope.clone());
        let hold = config
            .yellowstone_http_recovery
            .as_ref()
            .map(|_| Arc::new(std::sync::atomic::AtomicBool::new(restored.is_some())));
        let mut cursor = recovery::RecoveryCursor::new(scope, restored, limits.pending.clone())?;
        cursor.http_hold = hold;
        let task_cursor = cursor.clone();
        let (tx, rx) = queue::channel(limits.queue.count, limits.queue.bytes);
        let telemetry = Arc::new(DurableIngressTelemetry::default());
        let report = Arc::clone(&telemetry);
        let task_bot = bot_signer.clone();
        let task = tokio::spawn(async move {
            transport::run(
                Arc::new(runtime),
                limits,
                session,
                tx,
                task_bot,
                report,
                Some(task_cursor),
            )
            .await
        });
        Ok(Self {
            rx,
            task: Some(task),
            wallet_scope: Some(wallet_scope),
            bot_signer,
            telemetry,
            recovery: Some(cursor),
        })
    }
    /// Caller must supply only the SQLite commit/readback result, never received progress.
    pub fn acknowledge_checkpoint(
        &self,
        checkpoint: copybot_core_types::association_recovery::DurableCheckpoint,
    ) -> Result<()> {
        self.recovery
            .as_ref()
            .context("replay recovery disabled")?
            .acknowledge(checkpoint)
    }
    /// Shared fail-closed recovery barrier; independent from telemetry.
    pub fn http_continuity_hold(&self) -> Option<Arc<std::sync::atomic::AtomicBool>> {
        self.recovery
            .as_ref()
            .and_then(|cursor| cursor.http_hold.clone())
    }
    /// Runtime acknowledgement; call only after the parent envelope commits.
    pub fn acknowledge_parent(&self, slot: u64) {
        self.telemetry.acknowledge_parent(slot);
    }
    /// Record only after the delivery's SQLite write and readback succeeded.
    pub fn acknowledge_delivery(&self, elapsed: std::time::Duration) {
        self.telemetry.processing.durable_ack(elapsed);
    }
    pub fn ingress_snapshot(&self) -> DurableIngressSnapshot {
        self.telemetry.snapshot()
    }
    pub fn wallet_scope(&self) -> Option<&HashSet<String>> {
        self.wallet_scope.as_ref()
    }
    pub fn bot_signer(&self) -> Option<&str> {
        self.bot_signer.as_deref()
    }
    pub fn stop(&mut self) {
        if let Some(cursor) = &self.recovery {
            cursor.set_http_hold(true);
        }
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
        if let Some(cursor) = &self.recovery {
            cursor.set_http_hold(true);
        }
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}
