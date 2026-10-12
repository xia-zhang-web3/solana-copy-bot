//! Daemon scheduling for the explicit one-use owner execution scenario.
use crate::execution_canary::ExecutionCanaryRunner;
use crate::operator_emergency::OperatorEmergencyStop;
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::SqliteStore;
use std::future::Future;
use tokio::time::{self, Duration, MissedTickBehavior};

pub(crate) async fn run(
    store: SqliteStore,
    config: ExecutionConfig,
    heartbeat_seconds: u64,
) -> Result<()> {
    ensure!(copybot_config::owner_execution_roundtrip(&config)
        || copybot_config::owner_native_recovery_exit(&config),
        "execution_only_owner_policy_required");
    let runner = ExecutionCanaryRunner::new(config);
    run_until(store, runner, heartbeat_seconds, tokio::signal::ctrl_c()).await
}

pub(crate) async fn run_until<F>(
    store: SqliteStore,
    runner: ExecutionCanaryRunner,
    heartbeat_seconds: u64,
    stop: F,
) -> Result<()>
where
    F: Future<Output = std::io::Result<()>>,
{
    store.ensure_system_event_tables()?;
    runner.log_startup_status();
    let mut emergency = OperatorEmergencyStop::from_env();
    let mut heartbeat = time::interval(Duration::from_secs(heartbeat_seconds.max(1)));
    let mut execution = time::interval(Duration::from_secs(runner.interval_seconds()));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    execution.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tokio::pin!(stop);
    let outcome = async {
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    store.record_heartbeat("copybot-app", "execution_only")?;
                }
                _ = execution.tick() => {
                    emergency.refresh(&store, Utc::now())?;
                    if !emergency.is_active() {
                        let summary = runner.process_tick(&store, Utc::now()).await?;
                        if summary.has_status_change() {
                            crate::telemetry::record_execution_canary_tick(&summary);
                        }
                    }
                }
                result = &mut stop => {
                    result.context("execution-only stop signal")?;
                    break;
                }
            }
        }
        Ok(())
    }.await;
    store.record_heartbeat("copybot-app", "shutdown")?;
    outcome
}
