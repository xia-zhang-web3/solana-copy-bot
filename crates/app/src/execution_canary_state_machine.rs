use crate::execution_build_plan_metadata::{
    load_execution_build_plan_metadata, record_execution_build_plan_metadata,
};
use crate::execution_canary_entry_gate::validate_execution_canary_entry_metadata;
use crate::execution_canary_safety::pre_submit_safety_snapshot;
use crate::execution_canary_signing_contract::record_execution_signing_envelope;
use crate::execution_canary_submit_contract::record_execution_submit_plan;
use crate::execution_quote_canary_helpers::{quote_canary_slippage_limit_bps, SIDE_SELL};
use crate::execution_submit_adapter::{ExecutionSubmitAdapter, ExecutionSubmitRequest};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::{CopySignalRow, TokenQuantity};
use copybot_storage_core::{
    ExecutionCanaryRecordOutcome, SqliteStore, EXECUTION_CANARY_CONFIRM_DECISION_EXPIRE_UNSAFE,
    EXECUTION_CANARY_CONFIRM_DECISION_NOT_SUBMITTED, EXECUTION_CANARY_CONFIRM_DECISION_RETRY,
    EXECUTION_CANARY_CONFIRM_DECISION_WAIT, EXECUTION_CANARY_POSITION_CLOSE_CLOSED,
    EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED, EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION,
    EXECUTION_CANARY_POSITION_CLOSE_PARTIAL, EXECUTION_CANARY_SELL_DECISION_EXECUTE,
    EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT, EXECUTION_CANARY_SELL_DECISION_NO_POSITION,
    EXECUTION_ERROR_BUILD_FAILED, EXECUTION_ERROR_SIMULATION_FAILED,
    EXECUTION_SIMULATION_STATUS_FAILED, EXECUTION_STATUS_CANARY_EXPIRED,
    EXECUTION_STATUS_CANARY_FAILED, EXECUTION_STATUS_CANARY_SIMULATED,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionCanaryStateMachineSummary {
    pub(crate) candidates: usize,
    pub(crate) reserved: usize,
    pub(crate) existing: usize,
    pub(crate) built: usize,
    pub(crate) simulated: usize,
    pub(crate) signing_envelope_built: usize,
    pub(crate) failed: usize,
    pub(crate) submit_disabled: usize,
    pub(crate) submit_ready_rejected: usize,
    pub(crate) safety_blocked: usize,
    pub(crate) entry_gate_blocked: usize,
    pub(crate) open_positions: u64,
    pub(crate) daily_loss_sol: f64,
    pub(crate) expired: usize,
    pub(crate) submit_timeout_candidates: usize,
    pub(crate) submit_timeout_wait: usize,
    pub(crate) submit_timeout_retry: usize,
    pub(crate) submit_timeout_expire_unsafe: usize,
    pub(crate) submit_timeout_not_submitted: usize,
    pub(crate) sell_candidates: usize,
    pub(crate) sell_execute: usize,
    pub(crate) sell_force_exit: usize,
    pub(crate) sell_no_position: usize,
    pub(crate) sell_closed: usize,
    pub(crate) sell_partial: usize,
    pub(crate) sell_dust_closed: usize,
    pub(crate) skipped_reason: Option<&'static str>,
    pub(crate) last_sell_decision: Option<String>,
    pub(crate) last_close_status: Option<String>,
    pub(crate) last_closed_qty: f64,
    pub(crate) last_pnl_sol: f64,
    pub(crate) last_confirm_decision: Option<String>,
    pub(crate) last_confirm_reason: Option<String>,
    pub(crate) last_elapsed_seconds: i64,
    pub(crate) last_timeout_seconds: i64,
    pub(crate) last_order_id: Option<String>,
    pub(crate) last_signing_envelope_id: Option<String>,
    pub(crate) last_signing_envelope_mode: Option<String>,
    pub(crate) last_submit_idempotency_key: Option<String>,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionCanarySellCloseInput {
    pub(crate) target_qty: f64,
    pub(crate) target_qty_exact: Option<TokenQuantity>,
    pub(crate) exit_price_sol: f64,
    pub(crate) slippage_bps: Option<f64>,
    pub(crate) dust_qty_epsilon: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct ExecutionCanaryStateMachine<A> {
    config: ExecutionConfig,
    adapter: A,
}

impl<A: ExecutionSubmitAdapter> ExecutionCanaryStateMachine<A> {
    pub(crate) fn new(config: ExecutionConfig, adapter: A) -> Self {
        Self { config, adapter }
    }

    pub(crate) async fn process_buy_candidate(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        now: DateTime<Utc>,
    ) -> Result<ExecutionCanaryStateMachineSummary> {
        let mut summary = ExecutionCanaryStateMachineSummary::default();
        if !self.config.canary_enabled {
            summary.skipped_reason = Some("disabled");
            return Ok(summary);
        }
        if !self.config.canary_dry_run {
            return Err(anyhow!(
                "execution canary state machine requires canary_dry_run=true until submit is reviewed"
            ));
        }
        if !signal.side.eq_ignore_ascii_case("buy") {
            summary.skipped_reason = Some("not_buy");
            return Ok(summary);
        }

        summary.candidates = 1;
        let safety = pre_submit_safety_snapshot(&self.config, store, now)?;
        summary.open_positions = safety.open_positions;
        summary.daily_loss_sol = safety.daily_loss_sol;
        if let Some(reason) = safety.blocked_reason {
            summary.safety_blocked = 1;
            summary.skipped_reason = Some(reason);
            return Ok(summary);
        }
        if let Some(existing) = store.load_execution_canary_order_by_signal(&signal.signal_id)? {
            summary.existing = 1;
            summary.last_order_id = Some(existing.order_id);
            return Ok(summary);
        }

        let metadata = load_execution_build_plan_metadata(store, &signal.signal_id)?;
        if let Some(reason) = validate_execution_canary_entry_metadata(&self.config, &metadata) {
            summary.entry_gate_blocked = 1;
            summary.skipped_reason = Some(reason);
            return Ok(summary);
        }

        let reserve = store.reserve_execution_canary_order(
            &signal.signal_id,
            &self.config.canary_route,
            now,
        )?;
        summary.last_order_id = Some(reserve.order.order_id.clone());
        match reserve.outcome {
            ExecutionCanaryRecordOutcome::Existing => {
                summary.existing = 1;
                return Ok(summary);
            }
            ExecutionCanaryRecordOutcome::Inserted => {
                summary.reserved = 1;
            }
        }

        let request = ExecutionSubmitRequest {
            order_id: reserve.order.order_id.clone(),
            signal_id: signal.signal_id.clone(),
            client_order_id: reserve.order.client_order_id.clone(),
            attempt: reserve.order.attempt,
            route: self.config.canary_route.clone(),
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: signal.side.clone(),
            buy_size_sol: self.config.canary_buy_size_sol,
            slippage_tolerance_bps: quote_canary_slippage_limit_bps(
                &self.config,
                signal.side.as_str(),
            ),
            wallet_pubkey: self.config.canary_wallet_pubkey.clone(),
            metadata,
        };
        let plan = match self.adapter.build_transaction_plan(&request) {
            Ok(plan) => plan,
            Err(error) => {
                let error = error.to_string();
                let order = store.mark_execution_canary_failed(
                    &request.order_id,
                    now,
                    EXECUTION_ERROR_BUILD_FAILED,
                    &error,
                )?;
                if order.status == EXECUTION_STATUS_CANARY_FAILED {
                    summary.failed = 1;
                    summary.last_error = Some(error);
                }
                return Ok(summary);
            }
        };
        record_execution_build_plan_metadata(store, &plan, now)?;
        store.mark_execution_canary_built(&request.order_id, now)?;
        summary.built = 1;

        let simulation = match self.adapter.simulate_transaction_plan(&plan).await {
            Ok(simulation) => simulation,
            Err(error) => {
                let error = error.to_string();
                store.mark_execution_canary_simulated(
                    &request.order_id,
                    now,
                    EXECUTION_SIMULATION_STATUS_FAILED,
                    Some(&error),
                )?;
                let order = store.mark_execution_canary_failed(
                    &request.order_id,
                    now,
                    EXECUTION_ERROR_SIMULATION_FAILED,
                    &error,
                )?;
                if order.status == EXECUTION_STATUS_CANARY_FAILED {
                    summary.simulated = 1;
                    summary.failed = 1;
                    summary.last_error = Some(error);
                }
                return Ok(summary);
            }
        };
        store.mark_execution_canary_simulated(
            &request.order_id,
            now,
            &simulation.status,
            simulation.error.as_deref(),
        )?;
        summary.simulated = 1;
        if simulation.status == EXECUTION_SIMULATION_STATUS_FAILED {
            let error = simulation
                .error
                .unwrap_or_else(|| "simulation_failed".to_string());
            let order = store.mark_execution_canary_failed(
                &request.order_id,
                now,
                EXECUTION_ERROR_SIMULATION_FAILED,
                &error,
            )?;
            if order.status == EXECUTION_STATUS_CANARY_FAILED {
                summary.failed = 1;
                summary.last_error = Some(error);
            }
            return Ok(summary);
        }

        let signing_outcome =
            record_execution_signing_envelope(store, &self.adapter, &request, &plan, now)?;
        summary.signing_envelope_built = signing_outcome.built;
        summary.last_signing_envelope_id = signing_outcome.envelope_id;
        summary.last_signing_envelope_mode = signing_outcome.envelope_mode;
        summary.failed = signing_outcome.failed;
        summary.last_error = signing_outcome.error;
        if summary.failed > 0 {
            return Ok(summary);
        }
        let signing_envelope = signing_outcome.envelope.as_ref().expect("missing envelope");

        let submit_outcome =
            record_execution_submit_plan(store, &self.adapter, &request, signing_envelope, now)?;
        summary.failed = submit_outcome.failed;
        summary.submit_disabled = submit_outcome.submit_disabled;
        summary.submit_ready_rejected = submit_outcome.submit_ready_rejected;
        summary.skipped_reason = submit_outcome.skipped_reason;
        summary.last_submit_idempotency_key = submit_outcome.idempotency_key;
        summary.last_error = submit_outcome.error;
        Ok(summary)
    }

    pub(crate) fn process_sell_candidate(
        &self,
        store: &SqliteStore,
        signal: &CopySignalRow,
        close: ExecutionCanarySellCloseInput,
        now: DateTime<Utc>,
    ) -> Result<ExecutionCanaryStateMachineSummary> {
        let mut summary = ExecutionCanaryStateMachineSummary::default();
        if !self.config.canary_enabled {
            summary.skipped_reason = Some("disabled");
            return Ok(summary);
        }
        if !self.config.canary_dry_run {
            return Err(anyhow!(
                "execution canary state machine requires canary_dry_run=true until submit is reviewed"
            ));
        }
        if !signal.side.eq_ignore_ascii_case("sell") {
            summary.skipped_reason = Some("not_sell");
            return Ok(summary);
        }

        summary.sell_candidates = 1;
        let soft_slippage_limit_bps =
            quote_canary_slippage_limit_bps(&self.config, SIDE_SELL) as f64;
        let decision = store.execution_canary_sell_decision(
            &signal.token,
            close.slippage_bps,
            soft_slippage_limit_bps,
        )?;
        summary.last_sell_decision = Some(decision.decision_status.clone());
        match decision.decision_status.as_str() {
            EXECUTION_CANARY_SELL_DECISION_NO_POSITION => {
                summary.sell_no_position = 1;
                summary.skipped_reason = Some("no_owned_position");
                return Ok(summary);
            }
            EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT => summary.sell_force_exit = 1,
            EXECUTION_CANARY_SELL_DECISION_EXECUTE => summary.sell_execute = 1,
            _ => {
                summary.skipped_reason = Some("unsupported_sell_decision");
                return Ok(summary);
            }
        }

        let close_result = store.close_execution_canary_open_position(
            &signal.token,
            close.target_qty,
            close.target_qty_exact,
            close.exit_price_sol,
            close.dust_qty_epsilon,
            now,
        )?;
        summary.last_close_status = Some(close_result.close_status.clone());
        summary.last_closed_qty = close_result.closed_qty;
        summary.last_pnl_sol = close_result.pnl_sol;
        match close_result.close_status.as_str() {
            EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION => {
                summary.sell_no_position = 1;
                summary.skipped_reason = Some("no_owned_position");
            }
            EXECUTION_CANARY_POSITION_CLOSE_PARTIAL => {
                summary.sell_closed = 1;
                summary.sell_partial = 1;
            }
            EXECUTION_CANARY_POSITION_CLOSE_CLOSED => {
                summary.sell_closed = 1;
            }
            EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED => {
                summary.sell_closed = 1;
                summary.sell_dust_closed = 1;
            }
            _ => {
                summary.skipped_reason = Some("unsupported_close_status");
            }
        }
        Ok(summary)
    }

    pub(crate) fn process_submit_timeout(
        &self,
        store: &SqliteStore,
        order_id: &str,
        timeout: Duration,
        now: DateTime<Utc>,
    ) -> Result<ExecutionCanaryStateMachineSummary> {
        let mut summary = ExecutionCanaryStateMachineSummary::default();
        if !self.config.canary_enabled {
            summary.skipped_reason = Some("disabled");
            return Ok(summary);
        }
        if !self.config.canary_dry_run {
            return Err(anyhow!(
                "execution canary state machine requires canary_dry_run=true until submit is reviewed"
            ));
        }

        summary.submit_timeout_candidates = 1;
        summary.last_order_id = Some(order_id.to_string());
        let decision = store.execution_canary_confirm_timeout_decision(order_id, now, timeout)?;
        summary.last_confirm_decision = Some(decision.decision_status.clone());
        summary.last_confirm_reason = Some(decision.decision_reason.clone());
        summary.last_elapsed_seconds = decision.elapsed_seconds;
        summary.last_timeout_seconds = decision.timeout_seconds;

        match decision.decision_status.as_str() {
            EXECUTION_CANARY_CONFIRM_DECISION_WAIT => {
                summary.submit_timeout_wait = 1;
            }
            EXECUTION_CANARY_CONFIRM_DECISION_NOT_SUBMITTED => {
                summary.submit_timeout_not_submitted = 1;
                summary.skipped_reason = Some("not_submitted");
            }
            EXECUTION_CANARY_CONFIRM_DECISION_RETRY => {
                let order = store.mark_execution_canary_retry_after_submit_timeout(
                    order_id,
                    now,
                    timeout,
                    "retry_after_unknown_submit_timeout",
                )?;
                if order.status == EXECUTION_STATUS_CANARY_SIMULATED {
                    summary.submit_timeout_retry = 1;
                    summary.simulated = 1;
                }
            }
            EXECUTION_CANARY_CONFIRM_DECISION_EXPIRE_UNSAFE => {
                let order = store.mark_execution_canary_expired(
                    order_id,
                    now,
                    "confirm_timeout_with_signature_retry_unsafe",
                )?;
                if order.status == EXECUTION_STATUS_CANARY_EXPIRED {
                    summary.submit_timeout_expire_unsafe = 1;
                    summary.expired = 1;
                }
            }
            _ => {
                summary.skipped_reason = Some("unsupported_submit_timeout_decision");
            }
        }
        Ok(summary)
    }
}
