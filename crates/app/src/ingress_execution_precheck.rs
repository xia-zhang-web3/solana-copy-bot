//! Observation at the existing scheduler opportunity, before the real tick.
use crate::association_consumer::AssociationConsumer;
use crate::execution_canary::ExecutionCanaryRunner;
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::SqliteStore;

/// Periodic reconciliation remains the restart fallback. A newly committed
/// strict SELL also wakes execution without waiting for the next periodic tick.
/// Keep that hint across a continuity hold; the normal tick rechecks every gate.
pub(crate) struct Scheduler {
    interval: tokio::time::Interval,
    wake: Option<std::sync::Arc<tokio::sync::Notify>>,
    hold: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    sell_pending: bool,
}
impl Scheduler {
    pub(crate) fn new(
        runner: &ExecutionCanaryRunner,
        consumer: Option<&AssociationConsumer>,
    ) -> Self {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(runner.interval_seconds()));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            interval,
            wake: consumer.map(|c| c.execution_wake.clone()),
            hold: consumer.and_then(AssociationConsumer::http_continuity_hold),
            sell_pending: false,
        }
    }
    pub(crate) fn bind(&mut self, consumer: Option<&AssociationConsumer>) {
        self.wake = consumer.map(|c| c.execution_wake.clone());
        self.hold = consumer.and_then(AssociationConsumer::http_continuity_hold);
    }
    pub(crate) async fn ready(&mut self) {
        loop {
            let held = self
                .hold
                .as_ref()
                .is_some_and(|h| h.load(std::sync::atomic::Ordering::SeqCst));
            if self.sell_pending && !held {
                self.sell_pending = false;
                return;
            }
            tokio::select! {
                _ = self.interval.tick() => return,
                _ = async {
                    match &self.wake {
                        Some(wake) => wake.notified().await,
                        None => std::future::pending::<()>().await,
                    }
                } => self.sell_pending = true,
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)), if self.sell_pending => {},
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ExecutionPrecheck {
    pub phase: &'static str,
    pub execution_enabled: bool,
    pub canary_enabled: bool,
    pub tiny_submit_enabled: bool,
    pub quote_enabled: bool,
    pub scheduler_enabled: bool,
    pub process_tick_about_to_invoke: bool,
    pub continuity: Option<copybot_ingestion::ContinuityPrecheck>,
}
fn observe(
    config: &ExecutionConfig,
    scheduler_enabled: bool,
    consumer: Option<&AssociationConsumer>,
) -> ExecutionPrecheck {
    ExecutionPrecheck {
        phase: "before_process_tick_call",
        execution_enabled: config.enabled,
        canary_enabled: config.canary_enabled,
        tiny_submit_enabled: config.canary_tiny_submit_enabled,
        quote_enabled: config.quote_canary_enabled,
        scheduler_enabled,
        process_tick_about_to_invoke: scheduler_enabled,
        continuity: consumer.map(AssociationConsumer::continuity_precheck),
    }
}
pub(super) async fn tick(
    runner: &ExecutionCanaryRunner,
    store: &SqliteStore,
    config: &ExecutionConfig,
    consumer: Option<&AssociationConsumer>,
) {
    let observation = observe(config, runner.is_enabled(), consumer);
    record(&observation);
    match runner.process_tick(store, Utc::now()).await {
        Ok(summary) if summary.has_status_change() => {
            crate::telemetry::record_execution_canary_tick(&summary);
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(error = %error, "execution canary dry-run tick failed");
        }
    }
    #[cfg(test)]
    crate::app_tests::b70_hooks::execution_tick(consumer);
}
fn record(s: &ExecutionPrecheck) {
    if let Ok(encoded) = serde_json::to_string(&s.continuity) {
        tracing::info!(phase = s.phase, execution_enabled = s.execution_enabled, canary_enabled = s.canary_enabled,
            tiny_submit_enabled = s.tiny_submit_enabled, quote_enabled = s.quote_enabled,
            scheduler_enabled = s.scheduler_enabled, process_tick_about_to_invoke = s.process_tick_about_to_invoke,
            continuity_json = %encoded, "execution ingress precheck");
    }
}

#[cfg(test)]
#[path = "app_tests/ingress42_scheduler_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "app_tests/copy12_scheduler_tests.rs"]
mod sell_wake_tests;

#[cfg(test)]
#[path = "app_tests/copy12_saved_sell_replay_tests.rs"]
mod saved_sell_replay_tests;
