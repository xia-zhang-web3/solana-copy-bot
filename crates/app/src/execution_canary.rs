use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{ExecutionDryRunRecordOutcome, SqliteStore};
use std::path::Path;
use tracing::info;

const CANARY_COPY_SIGNAL_STATUS: &str = "shadow_recorded";

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionCanaryTickSummary {
    pub enabled: bool,
    pub dry_run: bool,
    pub route: String,
    pub wallet_pubkey: String,
    pub candidates: usize,
    pub inserted: usize,
    pub existing: usize,
    pub skipped_reason: Option<&'static str>,
    pub last_signal_id: Option<String>,
    pub last_order_id: Option<String>,
    pub last_error: Option<String>,
}

impl ExecutionCanaryTickSummary {
    pub(crate) fn has_status_change(&self) -> bool {
        self.inserted > 0 || self.existing > 0 || self.skipped_reason.is_some()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ExecutionCanaryRunner {
    config: ExecutionConfig,
}

impl ExecutionCanaryRunner {
    pub(crate) fn new(config: ExecutionConfig) -> Self {
        Self { config }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.config.canary_enabled
    }

    pub(crate) fn interval_seconds(&self) -> u64 {
        self.config.canary_interval_seconds.max(1)
    }

    pub(crate) fn log_startup_status(&self) {
        info!(
            enabled = self.config.canary_enabled,
            dry_run = self.config.canary_dry_run,
            route = %self.config.canary_route,
            wallet_pubkey = %self.config.canary_wallet_pubkey,
            buy_size_sol = self.config.canary_buy_size_sol,
            max_open_positions = self.config.canary_max_open_positions,
            max_daily_loss_sol = self.config.canary_max_daily_loss_sol,
            max_signal_age_seconds = self.config.canary_max_signal_age_seconds,
            kill_switch_path = %self.config.canary_kill_switch_path,
            "execution canary status"
        );
    }

    pub(crate) fn process_tick(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<ExecutionCanaryTickSummary> {
        let mut summary = ExecutionCanaryTickSummary {
            enabled: self.config.canary_enabled,
            dry_run: self.config.canary_dry_run,
            route: self.config.canary_route.clone(),
            wallet_pubkey: self.config.canary_wallet_pubkey.clone(),
            ..ExecutionCanaryTickSummary::default()
        };
        if !self.config.canary_enabled {
            summary.skipped_reason = Some("disabled");
            return Ok(summary);
        }
        if Path::new(&self.config.canary_kill_switch_path).exists() {
            summary.skipped_reason = Some("kill_switch_active");
            return Ok(summary);
        }

        let since = now - Duration::seconds(self.config.canary_max_signal_age_seconds as i64);
        let signals = store
            .list_execution_canary_candidates(
                CANARY_COPY_SIGNAL_STATUS,
                since,
                self.config.canary_batch_limit.max(1),
            )
            .context("failed loading execution canary candidates")?;
        summary.candidates = signals.len();
        for signal in signals {
            let outcome = store
                .record_execution_dry_run_order(&signal.signal_id, &self.config.canary_route, now)
                .with_context(|| {
                    format!(
                        "failed recording execution dry-run order for signal {}",
                        signal.signal_id
                    )
                })?;
            summary.last_signal_id = Some(signal.signal_id);
            match outcome {
                ExecutionDryRunRecordOutcome::Inserted => summary.inserted += 1,
                ExecutionDryRunRecordOutcome::Existing => summary.existing += 1,
            }
        }
        if let Some(order) = store
            .latest_execution_dry_run_order()
            .context("failed loading latest execution dry-run order")?
        {
            summary.last_order_id = Some(order.order_id);
        }
        Ok(summary)
    }
}
