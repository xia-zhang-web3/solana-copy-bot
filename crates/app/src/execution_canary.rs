use crate::execution_canary_state_machine::{
    ExecutionCanaryStateMachine, ExecutionCanaryStateMachineSummary,
};
use crate::execution_quote_canary::ExecutionQuoteCanaryRunner;
use crate::execution_submit_adapter::NoSubmitExecutionAdapter;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::{CopySignalRow, SwapEvent, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE};
use copybot_shadow::ShadowSignalResult;
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
    pub quote_entry_candidates: usize,
    pub quote_entry_inserted: usize,
    pub quote_entry_existing: usize,
    pub quote_entry_errors: usize,
    pub quote_close_candidates: usize,
    pub quote_close_inserted: usize,
    pub quote_close_existing: usize,
    pub quote_close_errors: usize,
    pub quote_would_execute: usize,
    pub quote_would_force_exit: usize,
    pub quote_would_skip: usize,
    pub quote_decision_unknown: usize,
    pub last_quote_event_id: Option<String>,
    pub state_machine_reserved: usize,
    pub state_machine_existing: usize,
    pub state_machine_built: usize,
    pub state_machine_simulated: usize,
    pub state_machine_submit_disabled: usize,
    pub state_machine_failed: usize,
    pub state_machine_safety_blocked: usize,
    pub last_state_machine_order_id: Option<String>,
}

impl ExecutionCanaryTickSummary {
    pub(crate) fn has_status_change(&self) -> bool {
        self.inserted > 0
            || self.existing > 0
            || self.skipped_reason.is_some()
            || self.quote_entry_inserted > 0
            || self.quote_entry_existing > 0
            || self.quote_entry_errors > 0
            || self.quote_close_inserted > 0
            || self.quote_close_existing > 0
            || self.quote_close_errors > 0
            || self.quote_would_execute > 0
            || self.quote_would_force_exit > 0
            || self.quote_would_skip > 0
            || self.quote_decision_unknown > 0
            || self.state_machine_reserved > 0
            || self.state_machine_existing > 0
            || self.state_machine_built > 0
            || self.state_machine_simulated > 0
            || self.state_machine_submit_disabled > 0
            || self.state_machine_failed > 0
            || self.state_machine_safety_blocked > 0
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ExecutionCanaryRunner {
    config: ExecutionConfig,
    quote_canary: ExecutionQuoteCanaryRunner,
}

impl ExecutionCanaryRunner {
    pub(crate) fn new(config: ExecutionConfig) -> Self {
        Self {
            quote_canary: ExecutionQuoteCanaryRunner::new(config.clone()),
            config,
        }
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
            quote_canary_enabled = self.config.quote_canary_enabled,
            quote_buy_size_sol = self.config.quote_canary_buy_size_sol,
            quote_slippage_bps = self.config.quote_canary_slippage_bps,
            quote_buy_slippage_bps = self.config.quote_canary_buy_slippage_bps,
            quote_sell_slippage_bps = self.config.quote_canary_sell_slippage_bps,
            quote_base_url_configured = !self.config.quote_canary_base_url.trim().is_empty(),
            quote_api_key_configured = !self.config.quote_canary_api_key.trim().is_empty(),
            priority_fee_canary_enabled = self.config.priority_fee_canary_enabled,
            priority_fee_rpc_url_configured = !self.config.priority_fee_canary_rpc_url.trim().is_empty(),
            "execution canary status"
        );
    }

    pub(crate) async fn process_tick(
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
        let signals = self.process_dry_run_orders(store, now, since, &mut summary)?;
        if self.quote_canary.is_enabled() {
            let quote_summary = self
                .quote_canary
                .process_tick(
                    store,
                    CANARY_COPY_SIGNAL_STATUS,
                    now,
                    since,
                    self.config.canary_batch_limit.max(1),
                )
                .await?;
            apply_quote_summary(&mut summary, quote_summary);
        }
        for signal in &signals {
            let state_summary = self.process_no_submit_state_machine(store, signal, now)?;
            apply_state_machine_summary(&mut summary, state_summary);
        }
        if let Some(order) = store
            .latest_execution_dry_run_order()
            .context("failed loading latest execution dry-run order")?
        {
            summary.last_order_id = Some(order.order_id);
        }
        Ok(summary)
    }

    pub(crate) async fn process_recorded_shadow_signal(
        &self,
        store: &SqliteStore,
        signal: &ShadowSignalResult,
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
        if signal.side == "buy" {
            summary.candidates = 1;
            let outcome = store
                .record_execution_dry_run_order(&signal.signal_id, &self.config.canary_route, now)
                .with_context(|| {
                    format!(
                        "failed recording execution dry-run order for hot shadow signal {}",
                        signal.signal_id
                    )
                })?;
            summary.last_signal_id = Some(signal.signal_id.clone());
            match outcome {
                ExecutionDryRunRecordOutcome::Inserted => summary.inserted += 1,
                ExecutionDryRunRecordOutcome::Existing => summary.existing += 1,
            }
        }
        if self.quote_canary.is_enabled() {
            let quote_summary = self
                .quote_canary
                .process_recorded_shadow_signal(store, signal, now)
                .await?;
            apply_quote_summary(&mut summary, quote_summary);
        }
        if signal.side == "buy" {
            let state_signal = copy_signal_from_shadow_signal(signal, now);
            let state_summary = self.process_no_submit_state_machine(store, &state_signal, now)?;
            apply_state_machine_summary(&mut summary, state_summary);
        }
        if let Some(order) = store
            .latest_execution_dry_run_order()
            .context("failed loading latest execution dry-run order")?
        {
            summary.last_order_id = Some(order.order_id);
        }
        Ok(summary)
    }

    pub(crate) async fn process_hot_observed_buy_quote(
        &self,
        store: &SqliteStore,
        swap: &SwapEvent,
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
        if self.quote_canary.is_enabled() {
            let quote_summary = self
                .quote_canary
                .process_hot_observed_buy_swap(store, swap, now)
                .await?;
            apply_quote_summary(&mut summary, quote_summary);
        }
        Ok(summary)
    }

    fn process_dry_run_orders(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        since: DateTime<Utc>,
        summary: &mut ExecutionCanaryTickSummary,
    ) -> Result<Vec<CopySignalRow>> {
        let signals = store
            .list_execution_canary_candidates(
                CANARY_COPY_SIGNAL_STATUS,
                since,
                self.config.canary_batch_limit.max(1),
            )
            .context("failed loading execution canary candidates")?;
        summary.candidates = signals.len();
        for signal in &signals {
            let outcome = store
                .record_execution_dry_run_order(&signal.signal_id, &self.config.canary_route, now)
                .with_context(|| {
                    format!(
                        "failed recording execution dry-run order for signal {}",
                        signal.signal_id
                    )
                })?;
            summary.last_signal_id = Some(signal.signal_id.clone());
            match outcome {
                ExecutionDryRunRecordOutcome::Inserted => summary.inserted += 1,
                ExecutionDryRunRecordOutcome::Existing => summary.existing += 1,
            }
        }
        Ok(signals)
    }

    fn process_no_submit_state_machine(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        now: DateTime<Utc>,
    ) -> Result<ExecutionCanaryStateMachineSummary> {
        let state_machine =
            ExecutionCanaryStateMachine::new(self.config.clone(), NoSubmitExecutionAdapter);
        state_machine.process_buy_candidate(store, signal, now)
    }
}

fn copy_signal_from_shadow_signal(
    signal: &ShadowSignalResult,
    now: DateTime<Utc>,
) -> CopySignalRow {
    CopySignalRow {
        signal_id: signal.signal_id.clone(),
        wallet_id: signal.wallet_id.clone(),
        side: signal.side.clone(),
        token: signal.token.clone(),
        notional_sol: signal.notional_sol,
        notional_lamports: None,
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
        ts: now,
        status: CANARY_COPY_SIGNAL_STATUS.to_string(),
    }
}

fn apply_quote_summary(
    summary: &mut ExecutionCanaryTickSummary,
    quote: crate::execution_quote_canary::ExecutionQuoteCanaryTickSummary,
) {
    summary.quote_entry_candidates = quote.entry_candidates;
    summary.quote_entry_inserted = quote.entry_inserted;
    summary.quote_entry_existing = quote.entry_existing;
    summary.quote_entry_errors = quote.entry_errors;
    summary.quote_close_candidates = quote.close_candidates;
    summary.quote_close_inserted = quote.close_inserted;
    summary.quote_close_existing = quote.close_existing;
    summary.quote_close_errors = quote.close_errors;
    summary.quote_would_execute = quote.would_execute;
    summary.quote_would_force_exit = quote.would_force_exit;
    summary.quote_would_skip = quote.would_skip;
    summary.quote_decision_unknown = quote.decision_unknown;
    summary.last_quote_event_id = quote.last_event_id;
}

fn apply_state_machine_summary(
    summary: &mut ExecutionCanaryTickSummary,
    state: ExecutionCanaryStateMachineSummary,
) {
    summary.state_machine_reserved += state.reserved;
    summary.state_machine_existing += state.existing;
    summary.state_machine_built += state.built;
    summary.state_machine_simulated += state.simulated;
    summary.state_machine_submit_disabled += state.submit_disabled;
    summary.state_machine_failed += state.failed;
    summary.state_machine_safety_blocked += state.safety_blocked;
    if state.last_order_id.is_some() {
        summary.last_state_machine_order_id = state.last_order_id;
    }
}
