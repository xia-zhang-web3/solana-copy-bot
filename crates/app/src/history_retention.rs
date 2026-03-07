use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use copybot_config::HistoryRetentionConfig;
use copybot_storage::{HistoryRetentionCutoffs, HistoryRetentionSummary, SqliteStore};

#[derive(Debug, Clone)]
pub(crate) struct HistoryRetentionRunner {
    config: HistoryRetentionConfig,
}

impl HistoryRetentionRunner {
    pub(crate) fn new(config: HistoryRetentionConfig) -> Self {
        Self { config }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.config.enabled
    }

    pub(crate) fn sweep_seconds(&self) -> u64 {
        self.config.sweep_seconds.max(1)
    }

    pub(crate) fn apply(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<HistoryRetentionSummary> {
        let protected_history_days = self.config.protected_history_days.max(1);
        let cutoffs = HistoryRetentionCutoffs {
            risk_events_before: now
                - ChronoDuration::days(
                    self.config.risk_events_days.max(protected_history_days) as i64,
                ),
            copy_signals_before: now
                - ChronoDuration::days(
                    self.config.copy_signals_days.max(protected_history_days) as i64,
                ),
            orders_before: now
                - ChronoDuration::days(self.config.orders_days.max(protected_history_days) as i64),
            shadow_closed_trades_before: now
                - ChronoDuration::days(
                    self.config
                        .shadow_closed_trades_days
                        .max(protected_history_days) as i64,
                ),
        };
        store.apply_history_retention(cutoffs)
    }
}
