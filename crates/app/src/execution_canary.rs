use crate::execution_canary_route::{
    list_swap_blueprint_state_machine_candidates, process_canary_state_machine_for_route,
    process_failed_sell_simulation_sweep, process_tiny_submit_orphan_position_recovery_sweep,
    process_tiny_submit_reconciliation_sweep, process_tiny_submit_sell_quote_event_for_route,
    uses_swap_blueprint_state_machine,
};
use crate::execution_canary_summary::{apply_quote_summary, apply_state_machine_summary};
use crate::execution_quote_canary::{ExecutionQuoteCanaryRunner, ExecutionQuoteCanaryTickSummary};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::{CopySignalRow, SwapEvent, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE};
use copybot_shadow::ShadowSignalResult;
use copybot_storage_core::{ExecutionDryRunRecordOutcome, SqliteStore};
use std::path::Path;
use tracing::info;

const CANARY_COPY_SIGNAL_STATUS: &str = "shadow_recorded";
const CLOSE_QUOTE_RETRY_LOOKBACK_SECONDS: i64 = 6 * 60 * 60;

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
    pub state_machine_entry_gate_blocked: usize,
    pub orphan_recovery_checked: usize,
    pub orphan_recovery_recovered: usize,
    pub orphan_recovery_skipped_no_history: usize,
    pub orphan_recovery_errors: usize,
    pub last_orphan_recovery_token: Option<String>,
    pub state_machine_skipped_reason: Option<&'static str>,
    pub state_machine_open_positions: u64,
    pub state_machine_daily_loss_sol: f64,
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
            || self.state_machine_entry_gate_blocked > 0
            || self.orphan_recovery_recovered > 0
            || self.orphan_recovery_errors > 0
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
            swap_instructions_dry_run_enabled = self.config.swap_instructions_dry_run_enabled,
            priority_fee_canary_enabled = self.config.priority_fee_canary_enabled,
            priority_fee_rpc_url_configured = !self.config.priority_fee_canary_rpc_url.trim().is_empty(),
            priority_fee_min_request_interval_ms = self.config.priority_fee_canary_min_request_interval_ms,
            priority_fee_cache_ttl_ms = self.config.priority_fee_canary_cache_ttl_ms,
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
        let signals = if uses_swap_blueprint_state_machine(&self.config) {
            if let Some(state) =
                process_tiny_submit_orphan_position_recovery_sweep(&self.config, store, now).await?
            {
                apply_state_machine_summary(&mut summary, state);
            }
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
                self.process_latest_close_quote_event(store, &quote_summary, now, &mut summary)
                    .await?;
                self.process_recent_close_quote_retries(store, now, &mut summary)
                    .await?;
                apply_quote_summary(&mut summary, quote_summary);
            }
            if let Some(state) =
                process_tiny_submit_reconciliation_sweep(&self.config, store, now).await?
            {
                apply_state_machine_summary(&mut summary, state);
            }
            if let Some(state) =
                process_failed_sell_simulation_sweep(&self.config, store, now).await?
            {
                apply_state_machine_summary(&mut summary, state);
            }
            let signals = list_swap_blueprint_state_machine_candidates(
                store,
                &self.config,
                CANARY_COPY_SIGNAL_STATUS,
                since,
            )?;
            summary.candidates = signals.len();
            if let Some(signal) = signals.last() {
                summary.last_signal_id = Some(signal.signal_id.clone());
            }
            signals
        } else {
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
                self.process_latest_close_quote_event(store, &quote_summary, now, &mut summary)
                    .await?;
                self.process_recent_close_quote_retries(store, now, &mut summary)
                    .await?;
                apply_quote_summary(&mut summary, quote_summary);
            }
            signals
        };
        for signal in &signals {
            let state_summary =
                process_canary_state_machine_for_route(&self.config, store, signal, now).await?;
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
            summary.last_signal_id = Some(signal.signal_id.clone());
            if !uses_swap_blueprint_state_machine(&self.config) {
                let outcome = store
                    .record_execution_dry_run_order(
                        &signal.signal_id,
                        &self.config.canary_route,
                        now,
                    )
                    .with_context(|| {
                        format!(
                            "failed recording execution dry-run order for hot shadow signal {}",
                            signal.signal_id
                        )
                    })?;
                match outcome {
                    ExecutionDryRunRecordOutcome::Inserted => summary.inserted += 1,
                    ExecutionDryRunRecordOutcome::Existing => summary.existing += 1,
                }
            }
        }
        if self.quote_canary.is_enabled() {
            let quote_summary = self
                .quote_canary
                .process_recorded_shadow_signal(store, signal, now)
                .await?;
            self.process_latest_close_quote_event(store, &quote_summary, now, &mut summary)
                .await?;
            apply_quote_summary(&mut summary, quote_summary);
        }
        if signal.side == "buy" {
            let state_signal = copy_signal_from_shadow_signal(signal, now);
            let state_summary =
                process_canary_state_machine_for_route(&self.config, store, &state_signal, now)
                    .await?;
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

    async fn process_latest_close_quote_event(
        &self,
        store: &SqliteStore,
        quote: &ExecutionQuoteCanaryTickSummary,
        now: DateTime<Utc>,
        summary: &mut ExecutionCanaryTickSummary,
    ) -> Result<()> {
        if !uses_swap_blueprint_state_machine(&self.config)
            || !self.config.canary_tiny_submit_enabled
        {
            return Ok(());
        }
        if quote.close_inserted == 0 && quote.close_existing == 0 {
            return Ok(());
        }
        let Some(event_id) = quote.last_event_id.as_deref() else {
            return Ok(());
        };
        if let Some(state) =
            process_tiny_submit_sell_quote_event_for_route(&self.config, store, event_id, now)
                .await?
        {
            apply_state_machine_summary(summary, state);
        }
        Ok(())
    }

    async fn process_recent_close_quote_retries(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        summary: &mut ExecutionCanaryTickSummary,
    ) -> Result<()> {
        if !uses_swap_blueprint_state_machine(&self.config)
            || !self.config.canary_tiny_submit_enabled
            || !self.quote_canary.is_enabled()
        {
            return Ok(());
        }
        let since = now - Duration::seconds(CLOSE_QUOTE_RETRY_LOOKBACK_SECONDS);
        let event_ids = store
            .list_execution_quote_canary_close_submit_retry_event_ids(
                since,
                self.config.canary_batch_limit.max(1),
            )
            .context("failed loading fresh close quote retry events")?;
        for event_id in event_ids {
            if let Some(state) =
                process_tiny_submit_sell_quote_event_for_route(&self.config, store, &event_id, now)
                    .await?
            {
                apply_state_machine_summary(summary, state);
            }
        }
        Ok(())
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
