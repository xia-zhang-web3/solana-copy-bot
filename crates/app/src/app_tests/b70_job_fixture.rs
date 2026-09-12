use super::{b58_fixture as base, *};
use crate::execution_quote_canary::job::HotQuoteAdmission;
use crate::execution_quote_canary::ExecutionQuoteCanaryRunner;
use std::sync::atomic::{AtomicUsize, Ordering};
static NEXT: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct Jobs {
    pub(in crate::app_tests) base: base::Fixture,
    pub config: ExecutionConfig,
    pub runner: ExecutionQuoteCanaryRunner,
    pub swap: SwapEvent,
}
impl Jobs {
    pub fn new(url: &str) -> Result<Self> {
        let base = base::Fixture::new(&format!(
            "b70-jobs-{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))?;
        let now = Utc::now();
        base.seed("TokenA", "buy", now)?;
        base.store.activate_follow_wallet(
            "leader-b58",
            now - chrono::Duration::seconds(1),
            "fixture",
        )?;
        let swap = base
            .store
            .load_recent_observed_swaps_in_window(now, now, 1)?
            .0
            .remove(0);
        let mut config = base::config(url.into());
        config.quote_canary_pump_fun_parallel_enabled = true;
        config.canary_enabled = true;
        config.canary_dry_run = true;
        config.canary_max_signal_age_seconds = 30;
        let runner = ExecutionQuoteCanaryRunner::new(config.clone());
        Ok(Self {
            base,
            config,
            runner,
            swap,
        })
    }
    pub fn store(&self) -> &copybot_storage_core::SqliteStore {
        &self.base.store
    }
    pub fn path(&self) -> &std::path::Path {
        &self.base.path
    }
    pub fn admission(&self) -> Result<HotQuoteAdmission> {
        self.runner
            .prepare_hot_quote(&self.base.store, &self.swap, Utc::now())?
            .context("expected admitted hot quote")
    }
    pub fn another(&self, n: usize) -> Result<HotQuoteAdmission> {
        let mut swap = self.swap.clone();
        swap.signature = format!("b70-job-{n}");
        self.base.store.insert_observed_swap(&swap)?;
        self.runner
            .prepare_hot_quote(&self.base.store, &swap, Utc::now())?
            .context("expected independent hot quote")
    }
    pub fn event(&self) -> Result<copybot_storage_core::ExecutionQuoteCanaryEventInsert> {
        self.base
            .store
            .load_latest_execution_quote_canary_entry_event(&base::signal_id("TokenA", "buy"))?
            .context("missing job quote event")
    }
    pub fn counts(&self) -> Result<(i64, i64)> {
        let conn = Connection::open(&self.base.path)?;
        Ok((
            conn.query_row(
                "SELECT count(*) FROM execution_quote_canary_events",
                [],
                |r| r.get(0),
            )?,
            conn.query_row(
                "SELECT count(*) FROM execution_quote_canary_provider_samples",
                [],
                |r| r.get(0),
            )?,
        ))
    }
}
