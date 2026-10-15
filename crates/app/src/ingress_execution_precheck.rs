//! Observation at the existing scheduler opportunity, before the real tick.
use crate::association_consumer::AssociationConsumer;
use crate::execution_canary::ExecutionCanaryRunner;
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::SqliteStore;

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
