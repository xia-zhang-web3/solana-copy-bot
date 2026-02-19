use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{ExecutionConfig, RiskConfig};
use copybot_storage::{
    CopySignalRow, FinalizeExecutionConfirmOutcome, InsertExecutionOrderPendingOutcome, SqliteStore,
};
use serde_json::json;
use std::collections::HashSet;
use tracing::{debug, warn};
use uuid::Uuid;

pub mod confirm;
pub mod idempotency;
pub mod intent;
pub mod pretrade;
pub mod reconcile;
pub mod simulator;
pub mod submitter;

use confirm::{
    ConfirmationStatus, FailClosedOrderConfirmer, OrderConfirmer, PaperOrderConfirmer,
    RpcOrderConfirmer,
};
use intent::{ExecutionIntent, ExecutionSide};
use pretrade::{
    FailClosedPreTradeChecker, PaperPreTradeChecker, PreTradeChecker, PreTradeDecisionKind,
    RpcPreTradeChecker,
};
use reconcile::build_fill;
use simulator::{IntentSimulator, PaperIntentSimulator};
use std::collections::BTreeMap;
use submitter::{
    AdapterOrderSubmitter, FailClosedOrderSubmitter, OrderSubmitter, PaperOrderSubmitter,
    SubmitErrorKind,
};

#[derive(Debug, Clone, Default)]
pub struct ExecutionBatchReport {
    pub attempted: u64,
    pub confirmed: u64,
    pub dropped: u64,
    pub failed: u64,
    pub skipped: u64,
    pub submit_attempted_by_route: BTreeMap<String, u64>,
    pub submit_retry_scheduled_by_route: BTreeMap<String, u64>,
    pub submit_failed_by_route: BTreeMap<String, u64>,
    pub pretrade_retry_scheduled_by_route: BTreeMap<String, u64>,
    pub pretrade_terminal_rejected_by_route: BTreeMap<String, u64>,
    pub pretrade_failed_by_route: BTreeMap<String, u64>,
    pub confirm_confirmed_by_route: BTreeMap<String, u64>,
    pub confirm_retry_scheduled_by_route: BTreeMap<String, u64>,
    pub confirm_failed_by_route: BTreeMap<String, u64>,
    pub confirm_latency_samples_by_route: BTreeMap<String, u64>,
    pub confirm_latency_ms_sum_by_route: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignalResult {
    Confirmed,
    Dropped,
    Failed,
    Skipped,
}

pub struct ExecutionRuntime {
    enabled: bool,
    mode: String,
    poll_interval_ms: u64,
    batch_size: u32,
    max_confirm_seconds: u64,
    max_submit_attempts: u32,
    max_copy_delay_sec: u64,
    default_route: String,
    submit_route_order: Vec<String>,
    route_tip_lamports: BTreeMap<String, u64>,
    slippage_bps: f64,
    simulate_before_submit: bool,
    manual_reconcile_required_on_confirm_failure: bool,
    risk: RiskConfig,
    pretrade: Box<dyn PreTradeChecker + Send + Sync>,
    simulator: Box<dyn IntentSimulator + Send + Sync>,
    submitter: Box<dyn OrderSubmitter + Send + Sync>,
    confirmer: Box<dyn OrderConfirmer + Send + Sync>,
}

impl ExecutionRuntime {
    pub fn from_config(config: ExecutionConfig, risk: RiskConfig) -> Self {
        let route = if config.default_route.trim().is_empty() {
            "paper".to_string()
        } else {
            config.default_route.trim().to_string()
        };
        let submit_route_order = build_submit_route_order(
            route.as_str(),
            &config.submit_allowed_routes,
            &config.submit_route_order,
        );
        let route_tip_lamports = normalize_route_tip_lamports(&config.submit_route_tip_lamports);

        let mode = config.mode.trim().to_ascii_lowercase();
        let (
            pretrade,
            simulator,
            submitter,
            confirmer,
            manual_reconcile_required_on_confirm_failure,
        ): (
            Box<dyn PreTradeChecker + Send + Sync>,
            Box<dyn IntentSimulator + Send + Sync>,
            Box<dyn OrderSubmitter + Send + Sync>,
            Box<dyn OrderConfirmer + Send + Sync>,
            bool,
        ) = match mode.as_str() {
            "paper" => (
                Box::new(PaperPreTradeChecker),
                Box::new(PaperIntentSimulator),
                Box::new(PaperOrderSubmitter),
                Box::new(PaperOrderConfirmer),
                false,
            ),
            "paper_rpc_confirm" => {
                let confirmer: Box<dyn OrderConfirmer + Send + Sync> = match RpcOrderConfirmer::new(
                    &config.rpc_http_url,
                    &config.rpc_fallback_http_url,
                    config.poll_interval_ms.max(500),
                ) {
                    Some(value) => Box::new(value),
                    None => {
                        warn!(
                            mode = "paper_rpc_confirm",
                            "rpc confirmer init failed; falling back to PaperOrderConfirmer"
                        );
                        Box::new(PaperOrderConfirmer)
                    }
                };
                (
                    Box::new(PaperPreTradeChecker),
                    Box::new(PaperIntentSimulator),
                    Box::new(PaperOrderSubmitter),
                    confirmer,
                    false,
                )
            }
            "paper_rpc_pretrade_confirm" => {
                let pretrade: Box<dyn PreTradeChecker + Send + Sync> = match RpcPreTradeChecker::new(
                    &config.rpc_http_url,
                    &config.rpc_fallback_http_url,
                    config.poll_interval_ms.max(500),
                    &config.execution_signer_pubkey,
                    config.pretrade_min_sol_reserve,
                    config.pretrade_require_token_account,
                    config.pretrade_max_priority_fee_lamports,
                ) {
                    Some(value) => Box::new(value),
                    None => Box::new(FailClosedPreTradeChecker::new(
                        "pretrade_checker_init_failed",
                        "rpc pre-trade checker init failed for paper_rpc_pretrade_confirm mode",
                    )),
                };
                let confirmer: Box<dyn OrderConfirmer + Send + Sync> = match RpcOrderConfirmer::new(
                    &config.rpc_http_url,
                    &config.rpc_fallback_http_url,
                    config.poll_interval_ms.max(500),
                ) {
                    Some(value) => Box::new(value),
                    None => {
                        warn!(
                            mode = "paper_rpc_pretrade_confirm",
                            "rpc confirmer init failed; falling back to PaperOrderConfirmer"
                        );
                        Box::new(PaperOrderConfirmer)
                    }
                };
                (
                    pretrade,
                    Box::new(PaperIntentSimulator),
                    Box::new(PaperOrderSubmitter),
                    confirmer,
                    false,
                )
            }
            "adapter_submit_confirm" => {
                let pretrade: Box<dyn PreTradeChecker + Send + Sync> = match RpcPreTradeChecker::new(
                    &config.rpc_http_url,
                    &config.rpc_fallback_http_url,
                    config.poll_interval_ms.max(500),
                    &config.execution_signer_pubkey,
                    config.pretrade_min_sol_reserve,
                    config.pretrade_require_token_account,
                    config.pretrade_max_priority_fee_lamports,
                ) {
                    Some(value) => Box::new(value),
                    None => Box::new(FailClosedPreTradeChecker::new(
                        "pretrade_checker_init_failed",
                        "rpc pre-trade checker init failed for adapter_submit_confirm mode",
                    )),
                };
                let submitter: Box<dyn OrderSubmitter + Send + Sync> =
                    match AdapterOrderSubmitter::new(
                        &config.submit_adapter_http_url,
                        &config.submit_adapter_fallback_http_url,
                        &config.submit_adapter_auth_token,
                        &config.submit_adapter_hmac_key_id,
                        &config.submit_adapter_hmac_secret,
                        config.submit_adapter_hmac_ttl_sec,
                        &config.submit_adapter_contract_version,
                        config.submit_adapter_require_policy_echo,
                        &config.submit_allowed_routes,
                        &config.submit_route_max_slippage_bps,
                        &config.submit_route_tip_lamports,
                        &config.submit_route_compute_unit_limit,
                        &config.submit_route_compute_unit_price_micro_lamports,
                        config.submit_timeout_ms.max(500),
                        config.slippage_bps,
                    ) {
                        Some(value) => Box::new(value),
                        None => Box::new(FailClosedOrderSubmitter::new(
                            "submitter_init_failed",
                            "adapter submitter init failed for adapter_submit_confirm mode",
                        )),
                    };
                let confirmer: Box<dyn OrderConfirmer + Send + Sync> = match RpcOrderConfirmer::new(
                    &config.rpc_http_url,
                    &config.rpc_fallback_http_url,
                    config.poll_interval_ms.max(500),
                ) {
                    Some(value) => Box::new(value),
                    None => Box::new(FailClosedOrderConfirmer::new(
                        "rpc confirmer init failed for adapter_submit_confirm mode",
                    )),
                };
                (
                    pretrade,
                    Box::new(PaperIntentSimulator),
                    submitter,
                    confirmer,
                    true,
                )
            }
            _ => (
                Box::new(PaperPreTradeChecker),
                Box::new(PaperIntentSimulator),
                Box::new(PaperOrderSubmitter),
                Box::new(PaperOrderConfirmer),
                false,
            ),
        };

        Self {
            enabled: config.enabled,
            mode,
            poll_interval_ms: config.poll_interval_ms.max(100),
            batch_size: config.batch_size.max(1),
            max_confirm_seconds: config.max_confirm_seconds.max(1),
            max_submit_attempts: config.max_submit_attempts.max(1),
            max_copy_delay_sec: risk.max_copy_delay_sec.max(1),
            default_route: route,
            submit_route_order,
            route_tip_lamports,
            slippage_bps: config.slippage_bps,
            simulate_before_submit: config.simulate_before_submit,
            manual_reconcile_required_on_confirm_failure,
            risk,
            pretrade,
            simulator,
            submitter,
            confirmer,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn poll_interval_ms(&self) -> u64 {
        self.poll_interval_ms
    }

    pub fn process_batch(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        buy_submit_pause_reason: Option<&str>,
    ) -> Result<ExecutionBatchReport> {
        if !self.enabled {
            return Ok(ExecutionBatchReport::default());
        }

        let mut seen = HashSet::new();
        let mut candidates = Vec::new();
        let prioritize_sell_pre_submit = buy_submit_pause_reason.is_some();
        for status in [
            "execution_submitted",
            "execution_simulated",
            "execution_pending",
            "shadow_recorded",
        ] {
            let prioritize_sell_for_status =
                prioritize_sell_pre_submit && self.is_pre_submit_status(status);
            for signal in store.list_copy_signals_by_status_with_side_priority(
                status,
                self.batch_size,
                prioritize_sell_for_status,
            )? {
                if prioritize_sell_for_status && signal.side.eq_ignore_ascii_case("buy") {
                    continue;
                }
                if seen.insert(signal.signal_id.clone()) {
                    candidates.push(signal);
                }
                if candidates.len() >= self.batch_size as usize {
                    break;
                }
            }
            if candidates.len() >= self.batch_size as usize {
                break;
            }
        }

        let mut report = ExecutionBatchReport::default();
        for signal in candidates {
            report.attempted = report.attempted.saturating_add(1);
            match self.process_one_signal(store, signal, now, buy_submit_pause_reason, &mut report)
            {
                Ok(SignalResult::Confirmed) => {
                    report.confirmed = report.confirmed.saturating_add(1)
                }
                Ok(SignalResult::Dropped) => report.dropped = report.dropped.saturating_add(1),
                Ok(SignalResult::Failed) => report.failed = report.failed.saturating_add(1),
                Ok(SignalResult::Skipped) => report.skipped = report.skipped.saturating_add(1),
                Err(error) => {
                    report.failed = report.failed.saturating_add(1);
                    warn!(error = %error, "execution pipeline failed for signal");
                }
            }
        }

        Ok(report)
    }

    fn process_one_signal(
        &self,
        store: &SqliteStore,
        signal: CopySignalRow,
        now: DateTime<Utc>,
        buy_submit_pause_reason: Option<&str>,
        report: &mut ExecutionBatchReport,
    ) -> Result<SignalResult> {
        let intent = match ExecutionIntent::try_from(signal.clone()) {
            Ok(value) => value,
            Err(error) => {
                store.update_copy_signal_status(&signal.signal_id, "execution_failed")?;
                let details = json!({
                    "signal_id": signal.signal_id,
                    "reason": "invalid_intent",
                    "error": error.to_string()
                })
                .to_string();
                let _ = store.insert_risk_event(
                    "execution_invalid_intent",
                    "error",
                    now,
                    Some(&details),
                );
                return Ok(SignalResult::Failed);
            }
        };

        let attempt = 1;
        let client_order_id = idempotency::client_order_id(&intent.signal_id, attempt);
        let mut order = store.execution_order_by_client_order_id(&client_order_id)?;
        if let Some(existing) = order.as_ref() {
            if signal.status != existing.status {
                store.update_copy_signal_status(&intent.signal_id, existing.status.as_str())?;
            }
            debug!(
                signal_id = %intent.signal_id,
                client_order_id = %client_order_id,
                existing_status = %existing.status,
                "execution idempotency hit"
            );
        }

        let lifecycle_status = order
            .as_ref()
            .map(|value| value.status.as_str())
            .unwrap_or(signal.status.as_str());

        if self.should_pause_buy_submission(&intent, lifecycle_status, buy_submit_pause_reason) {
            return Ok(SignalResult::Skipped);
        }

        if self.is_pre_submit_status(lifecycle_status) {
            if now - intent.signal_ts > Duration::seconds(self.max_copy_delay_sec as i64) {
                if let Some(existing) = order.as_ref() {
                    let _ = store.mark_order_dropped(&existing.order_id, "signal_stale", None);
                }
                store.update_copy_signal_status(&intent.signal_id, "execution_dropped")?;
                let details = json!({
                    "signal_id": intent.signal_id,
                    "reason": "signal_stale",
                    "max_copy_delay_sec": self.max_copy_delay_sec,
                })
                .to_string();
                let _ =
                    store.insert_risk_event("execution_signal_stale", "warn", now, Some(&details));
                return Ok(SignalResult::Dropped);
            }

            if let Some(reason) = self.execution_risk_block_reason(store, &intent)? {
                if let Some(existing) = order.as_ref() {
                    let _ = store.mark_order_dropped(
                        &existing.order_id,
                        "risk_blocked",
                        Some(reason.as_str()),
                    );
                }
                store.update_copy_signal_status(&intent.signal_id, "execution_dropped")?;
                let details = json!({
                    "signal_id": intent.signal_id,
                    "reason": reason,
                    "token": intent.token,
                    "notional_sol": intent.notional_sol,
                })
                .to_string();
                let _ =
                    store.insert_risk_event("execution_risk_block", "warn", now, Some(&details));
                return Ok(SignalResult::Dropped);
            }
        }

        if order.is_none() {
            let order_id = format!("ord:{}", Uuid::new_v4().simple());
            let insert_outcome = store.insert_execution_order_pending(
                &order_id,
                &intent.signal_id,
                &client_order_id,
                &self.default_route,
                now,
                attempt,
            )?;
            if matches!(insert_outcome, InsertExecutionOrderPendingOutcome::Inserted) {
                store.update_copy_signal_status(&intent.signal_id, "execution_pending")?;
            }
            order = store.execution_order_by_client_order_id(&client_order_id)?;
            if order.is_none() {
                return Ok(SignalResult::Skipped);
            }
        }

        let order = order.context("missing order after idempotency/order create step")?;
        match order.status.as_str() {
            "execution_confirmed" => {
                store.update_copy_signal_status(&intent.signal_id, "execution_confirmed")?;
                Ok(SignalResult::Confirmed)
            }
            "execution_dropped" => {
                store.update_copy_signal_status(&intent.signal_id, "execution_dropped")?;
                Ok(SignalResult::Dropped)
            }
            "execution_failed" => {
                store.update_copy_signal_status(&intent.signal_id, "execution_failed")?;
                Ok(SignalResult::Failed)
            }
            "execution_pending" => {
                self.process_pending_order(store, &intent, &client_order_id, &order, now, report)
            }
            "execution_simulated" => {
                self.process_simulated_order(store, &intent, &client_order_id, &order, now, report)
            }
            "execution_submitted" => {
                self.process_submitted_order(store, &intent, &order, now, report)
            }
            other => {
                let details = json!({
                    "signal_id": intent.signal_id,
                    "order_id": order.order_id,
                    "status": other,
                    "reason": "unknown_order_status"
                })
                .to_string();
                let _ = store.insert_risk_event(
                    "execution_unknown_order_status",
                    "error",
                    now,
                    Some(&details),
                );
                store.mark_order_failed(&order.order_id, "unknown_order_status", Some(other))?;
                store.update_copy_signal_status(&intent.signal_id, "execution_failed")?;
                Ok(SignalResult::Failed)
            }
        }
    }

    fn process_pending_order(
        &self,
        store: &SqliteStore,
        intent: &ExecutionIntent,
        client_order_id: &str,
        order: &copybot_storage::ExecutionOrderRow,
        now: DateTime<Utc>,
        report: &mut ExecutionBatchReport,
    ) -> Result<SignalResult> {
        let attempt = order.attempt.max(1);
        let selected_route = self.submit_route_for_attempt(attempt);
        let pretrade = self
            .pretrade
            .check(intent, selected_route)
            .with_context(|| format!("failed pre-trade checks for signal {}", intent.signal_id))?;
        match pretrade.kind {
            PreTradeDecisionKind::Allow => {}
            PreTradeDecisionKind::RetryableReject => {
                let reason_code = pretrade.reason_code;
                let detail_text = pretrade.detail;
                let detail = format!("{}: {}", reason_code, detail_text);
                if attempt < self.max_submit_attempts {
                    let next_attempt = attempt.saturating_add(1);
                    store.set_order_attempt(&order.order_id, next_attempt, Some(&detail))?;
                    store.update_copy_signal_status(&intent.signal_id, "execution_pending")?;
                    bump_route_counter(
                        &mut report.pretrade_retry_scheduled_by_route,
                        selected_route,
                    );
                    let details = json!({
                        "signal_id": intent.signal_id,
                        "order_id": order.order_id,
                        "attempt": attempt,
                        "next_attempt": next_attempt,
                        "max_submit_attempts": self.max_submit_attempts,
                        "route": selected_route,
                        "reason_code": reason_code,
                        "detail": detail_text,
                    })
                    .to_string();
                    let _ = store.insert_risk_event(
                        "execution_pretrade_retry_scheduled",
                        "warn",
                        now,
                        Some(&details),
                    );
                    return Ok(SignalResult::Skipped);
                }
                store.mark_order_failed(
                    &order.order_id,
                    "pretrade_attempts_exhausted",
                    Some(&detail),
                )?;
                store.update_copy_signal_status(&intent.signal_id, "execution_failed")?;
                bump_route_counter(&mut report.pretrade_failed_by_route, selected_route);
                let details = json!({
                    "signal_id": intent.signal_id,
                    "order_id": order.order_id,
                    "attempt": attempt,
                    "max_submit_attempts": self.max_submit_attempts,
                    "route": selected_route,
                    "reason_code": reason_code,
                    "detail": detail_text,
                })
                .to_string();
                let _ = store.insert_risk_event(
                    "execution_pretrade_failed",
                    "error",
                    now,
                    Some(&details),
                );
                return Ok(SignalResult::Failed);
            }
            PreTradeDecisionKind::TerminalReject => {
                let reason_code = pretrade.reason_code;
                let detail_text = pretrade.detail;
                let detail = format!("{}: {}", reason_code, detail_text);
                store.mark_order_dropped(&order.order_id, "pretrade_rejected", Some(&detail))?;
                store.update_copy_signal_status(&intent.signal_id, "execution_dropped")?;
                bump_route_counter(
                    &mut report.pretrade_terminal_rejected_by_route,
                    selected_route,
                );
                let details = json!({
                    "signal_id": intent.signal_id,
                    "order_id": order.order_id,
                    "route": selected_route,
                    "reason_code": reason_code,
                    "detail": detail_text,
                })
                .to_string();
                let _ = store.insert_risk_event(
                    "execution_pretrade_rejected",
                    "warn",
                    now,
                    Some(&details),
                );
                return Ok(SignalResult::Dropped);
            }
        }

        if self.simulate_before_submit {
            let sim = self.simulator.simulate(intent).with_context(|| {
                format!("failed to simulate order for signal {}", intent.signal_id)
            })?;
            if !sim.accepted {
                store.mark_order_dropped(
                    &order.order_id,
                    "simulation_rejected",
                    Some(sim.detail.as_str()),
                )?;
                store.update_copy_signal_status(&intent.signal_id, "execution_dropped")?;
                return Ok(SignalResult::Dropped);
            }
            store.mark_order_simulated(&order.order_id, "ok", Some(sim.detail.as_str()))?;
            store.update_copy_signal_status(&intent.signal_id, "execution_simulated")?;
        }

        self.process_simulated_order(store, intent, client_order_id, order, now, report)
    }

    fn process_simulated_order(
        &self,
        store: &SqliteStore,
        intent: &ExecutionIntent,
        client_order_id: &str,
        order: &copybot_storage::ExecutionOrderRow,
        now: DateTime<Utc>,
        report: &mut ExecutionBatchReport,
    ) -> Result<SignalResult> {
        let attempt = order.attempt.max(1);
        if attempt > self.max_submit_attempts {
            let detail = format!(
                "attempt={} exceeds max_submit_attempts={}",
                attempt, self.max_submit_attempts
            );
            store.mark_order_failed(&order.order_id, "submit_attempts_exhausted", Some(&detail))?;
            store.update_copy_signal_status(&intent.signal_id, "execution_failed")?;
            return Ok(SignalResult::Failed);
        }

        let selected_route = self.submit_route_for_attempt(attempt);
        bump_route_counter(&mut report.submit_attempted_by_route, selected_route);
        let submit = match self
            .submitter
            .submit(intent, client_order_id, selected_route)
        {
            Ok(value) => value,
            Err(error) => {
                let retryable = matches!(error.kind, SubmitErrorKind::Retryable);
                let detail = format!(
                    "submit_error attempt={} max_attempts={} route={} code={} detail={}",
                    attempt, self.max_submit_attempts, selected_route, error.code, error.detail
                );
                if retryable && attempt < self.max_submit_attempts {
                    let next_attempt = attempt.saturating_add(1);
                    store.set_order_attempt(&order.order_id, next_attempt, Some(&detail))?;
                    store.update_copy_signal_status(&intent.signal_id, "execution_simulated")?;
                    bump_route_counter(&mut report.submit_retry_scheduled_by_route, selected_route);
                    let details = json!({
                        "signal_id": intent.signal_id,
                        "order_id": order.order_id,
                        "attempt": attempt,
                        "next_attempt": next_attempt,
                        "max_submit_attempts": self.max_submit_attempts,
                        "route": selected_route,
                        "error_code": error.code,
                        "retryable": true,
                        "error": error.detail
                    })
                    .to_string();
                    let _ = store.insert_risk_event(
                        "execution_submit_retry_scheduled",
                        "warn",
                        now,
                        Some(&details),
                    );
                    return Ok(SignalResult::Skipped);
                }

                let err_code = if retryable {
                    "submit_attempts_exhausted"
                } else {
                    "submit_terminal_rejected"
                };
                bump_route_counter(&mut report.submit_failed_by_route, selected_route);
                store.mark_order_failed(&order.order_id, err_code, Some(&detail))?;
                store.update_copy_signal_status(&intent.signal_id, "execution_failed")?;
                let details = json!({
                    "signal_id": intent.signal_id,
                    "order_id": order.order_id,
                    "attempt": attempt,
                    "max_submit_attempts": self.max_submit_attempts,
                    "route": selected_route,
                    "error_code": error.code,
                    "retryable": retryable,
                    "error": error.detail
                })
                .to_string();
                let _ = store.insert_risk_event(
                    "execution_submit_failed",
                    "error",
                    now,
                    Some(&details),
                );
                return Ok(SignalResult::Failed);
            }
        };
        store.mark_order_submitted(
            &order.order_id,
            submit.route.as_str(),
            submit.tx_signature.as_str(),
            submit.submitted_at,
        )?;
        store.update_copy_signal_status(&intent.signal_id, "execution_submitted")?;
        self.process_submitted_order_by_signature(
            store,
            intent,
            &order.order_id,
            submit.route.as_str(),
            submit.tx_signature.as_str(),
            submit.submitted_at,
            now,
            report,
        )
    }

    fn process_submitted_order(
        &self,
        store: &SqliteStore,
        intent: &ExecutionIntent,
        order: &copybot_storage::ExecutionOrderRow,
        now: DateTime<Utc>,
        report: &mut ExecutionBatchReport,
    ) -> Result<SignalResult> {
        let tx_signature = order.tx_signature.as_deref().unwrap_or_default().trim();
        if tx_signature.is_empty() {
            store.mark_order_failed(
                &order.order_id,
                "missing_tx_signature",
                Some("execution_submitted order missing tx_signature"),
            )?;
            store.update_copy_signal_status(&intent.signal_id, "execution_failed")?;
            bump_route_counter(&mut report.confirm_failed_by_route, order.route.as_str());
            return Ok(SignalResult::Failed);
        }
        self.process_submitted_order_by_signature(
            store,
            intent,
            &order.order_id,
            order.route.as_str(),
            tx_signature,
            order.submit_ts,
            now,
            report,
        )
    }

    fn process_submitted_order_by_signature(
        &self,
        store: &SqliteStore,
        intent: &ExecutionIntent,
        order_id: &str,
        route: &str,
        tx_signature: &str,
        submit_ts: DateTime<Utc>,
        now: DateTime<Utc>,
        report: &mut ExecutionBatchReport,
    ) -> Result<SignalResult> {
        let deadline = submit_ts + Duration::seconds(self.max_confirm_seconds as i64);
        let confirm = match self.confirmer.confirm(tx_signature, deadline) {
            Ok(value) => value,
            Err(error) => {
                if now < deadline {
                    bump_route_counter(&mut report.confirm_retry_scheduled_by_route, route);
                    warn!(
                        signal_id = %intent.signal_id,
                        order_id,
                        error = %error,
                        "confirm attempt failed; will retry until deadline"
                    );
                    return Ok(SignalResult::Skipped);
                }
                let manual_reconcile_required = self.manual_reconcile_required_on_confirm_failure;
                let err_code = if manual_reconcile_required {
                    "confirm_error_manual_reconcile_required"
                } else {
                    "confirm_error"
                };
                let detail = format!(
                    "confirm_error deadline_passed mode={} manual_reconcile_required={} error={}",
                    self.mode, manual_reconcile_required, error
                );
                store.mark_order_failed(order_id, err_code, Some(&detail))?;
                store.update_copy_signal_status(&intent.signal_id, "execution_failed")?;
                bump_route_counter(&mut report.confirm_failed_by_route, route);
                let details = json!({
                    "signal_id": intent.signal_id,
                    "order_id": order_id,
                    "mode": self.mode,
                    "manual_reconcile_required": manual_reconcile_required,
                    "deadline": deadline.to_rfc3339(),
                    "error": error.to_string()
                })
                .to_string();
                let _ = store.insert_risk_event(
                    if manual_reconcile_required {
                        "execution_confirm_failed_manual_reconcile_required"
                    } else {
                        "execution_confirm_failed"
                    },
                    "error",
                    now,
                    Some(&details),
                );
                return Ok(SignalResult::Failed);
            }
        };

        match confirm.status {
            ConfirmationStatus::Confirmed => {
                let confirmed_at = confirm.confirmed_at.unwrap_or_else(Utc::now);
                let route_tip_lamports = self.route_tip_lamports(route);
                let execution_fee_sol = fee_sol_from_lamports(
                    confirm.network_fee_lamports.unwrap_or(0),
                    route_tip_lamports,
                );
                let (avg_price_sol, used_price_fallback, fallback_source) = match store
                    .latest_token_sol_price(&intent.token, confirmed_at)?
                {
                    Some(price) if price.is_finite() && price > 0.0 => {
                        (price.max(1e-9), false, None)
                    }
                    _ => {
                        let open_position_avg_cost = store
                            .live_open_position_qty_cost(&intent.token)?
                            .and_then(|(qty, cost_sol)| {
                                if qty > 1e-9 && cost_sol > 0.0 {
                                    Some((cost_sol / qty).max(1e-9))
                                } else {
                                    None
                                }
                            });
                        let (fallback, source) = fallback_price_and_source(open_position_avg_cost);
                        warn!(
                            signal_id = %intent.signal_id,
                            token = %intent.token,
                            route,
                            fallback_avg_price_sol = fallback,
                            fallback_source = %source,
                            "latest token price unavailable; using fallback price to keep confirmed reconcile/exposure consistent"
                        );
                        (fallback, true, Some(source))
                    }
                };
                let fill = build_fill(
                    intent,
                    order_id,
                    avg_price_sol,
                    self.slippage_bps,
                    execution_fee_sol,
                )?;
                match store.finalize_execution_confirmed_order(
                    &fill.order_id,
                    &intent.signal_id,
                    &fill.token,
                    intent.side.as_str(),
                    fill.qty,
                    intent.notional_sol,
                    fill.avg_price_sol,
                    fill.fee_sol,
                    fill.slippage_bps,
                    confirmed_at,
                )? {
                    FinalizeExecutionConfirmOutcome::Applied => {}
                    FinalizeExecutionConfirmOutcome::AlreadyConfirmed => {
                        store
                            .update_copy_signal_status(&intent.signal_id, "execution_confirmed")?;
                    }
                }
                if confirm.network_fee_lamports.is_none() && self.mode == "adapter_submit_confirm" {
                    let details = json!({
                        "signal_id": intent.signal_id,
                        "order_id": order_id,
                        "route": route,
                        "network_fee_lamports": null,
                        "tip_lamports": route_tip_lamports,
                        "fee_sol_applied": execution_fee_sol,
                        "reason": "missing_network_fee_from_confirmation",
                        "manual_reconcile_recommended": true
                    })
                    .to_string();
                    let _ = store.insert_risk_event(
                        "execution_network_fee_unavailable_fallback_used",
                        "error",
                        now,
                        Some(&details),
                    );
                }
                if used_price_fallback {
                    let details = json!({
                        "signal_id": intent.signal_id,
                        "order_id": order_id,
                        "token": intent.token,
                        "route": route,
                        "fallback_avg_price_sol": avg_price_sol,
                        "fallback_source": fallback_source.unwrap_or_else(|| "unknown".to_string()),
                        "reason": "missing_latest_price_runtime_fallback",
                        "manual_reconcile_recommended": true
                    })
                    .to_string();
                    let _ = store.insert_risk_event(
                        "execution_price_unavailable_fallback_used",
                        "error",
                        now,
                        Some(&details),
                    );
                }
                bump_route_counter(&mut report.confirm_confirmed_by_route, route);
                bump_route_counter(&mut report.confirm_latency_samples_by_route, route);
                let confirm_latency_ms = confirmed_at
                    .signed_duration_since(submit_ts)
                    .num_milliseconds()
                    .max(0) as u64;
                accumulate_route_sum(
                    &mut report.confirm_latency_ms_sum_by_route,
                    route,
                    confirm_latency_ms,
                );
                Ok(SignalResult::Confirmed)
            }
            ConfirmationStatus::Failed => {
                store.mark_order_failed(
                    order_id,
                    "confirm_rejected",
                    Some(confirm.detail.as_str()),
                )?;
                store.update_copy_signal_status(&intent.signal_id, "execution_failed")?;
                bump_route_counter(&mut report.confirm_failed_by_route, route);
                Ok(SignalResult::Failed)
            }
            ConfirmationStatus::Timeout => {
                if now < deadline {
                    bump_route_counter(&mut report.confirm_retry_scheduled_by_route, route);
                    return Ok(SignalResult::Skipped);
                }
                let manual_reconcile_required = self.manual_reconcile_required_on_confirm_failure;
                let err_code = if manual_reconcile_required {
                    "confirm_timeout_manual_reconcile_required"
                } else {
                    "confirm_timeout"
                };
                store.mark_order_failed(order_id, err_code, Some(confirm.detail.as_str()))?;
                store.update_copy_signal_status(&intent.signal_id, "execution_failed")?;
                bump_route_counter(&mut report.confirm_failed_by_route, route);
                if manual_reconcile_required {
                    let details = json!({
                        "signal_id": intent.signal_id,
                        "order_id": order_id,
                        "mode": self.mode,
                        "manual_reconcile_required": true,
                        "deadline": deadline.to_rfc3339(),
                        "detail": confirm.detail,
                    })
                    .to_string();
                    let _ = store.insert_risk_event(
                        "execution_confirm_timeout_manual_reconcile_required",
                        "error",
                        now,
                        Some(&details),
                    );
                }
                Ok(SignalResult::Failed)
            }
        }
    }

    fn submit_route_for_attempt(&self, attempt: u32) -> &str {
        if self.submit_route_order.is_empty() {
            return self.default_route.as_str();
        }
        let index = attempt.saturating_sub(1) as usize;
        let index = index.min(self.submit_route_order.len().saturating_sub(1));
        self.submit_route_order[index].as_str()
    }

    fn route_tip_lamports(&self, route: &str) -> u64 {
        normalize_route(route)
            .and_then(|value| self.route_tip_lamports.get(value.as_str()).copied())
            .unwrap_or(0)
    }

    fn should_pause_buy_submission(
        &self,
        intent: &ExecutionIntent,
        lifecycle_status: &str,
        buy_submit_pause_reason: Option<&str>,
    ) -> bool {
        if !matches!(intent.side, ExecutionSide::Buy) {
            return false;
        }
        if !self.is_pre_submit_status(lifecycle_status) {
            return false;
        }
        if let Some(reason) = buy_submit_pause_reason {
            debug!(
                signal_id = %intent.signal_id,
                token = %intent.token,
                reason,
                "buy execution paused by runtime gate"
            );
            return true;
        }
        false
    }

    fn is_pre_submit_status(&self, status: &str) -> bool {
        matches!(
            status,
            "shadow_recorded" | "execution_pending" | "execution_simulated"
        )
    }

    fn execution_risk_block_reason(
        &self,
        store: &SqliteStore,
        intent: &ExecutionIntent,
    ) -> Result<Option<String>> {
        match intent.side {
            ExecutionSide::Sell => {
                if !store.live_has_open_position(&intent.token)? {
                    return Ok(Some(format!(
                        "sell_without_open_position token={}",
                        intent.token
                    )));
                }
                Ok(None)
            }
            ExecutionSide::Buy => {
                if intent.notional_sol > self.risk.max_position_sol {
                    return Ok(Some(format!(
                        "max_position_sol_exceeded notional_sol={:.6} max_position_sol={:.6}",
                        intent.notional_sol, self.risk.max_position_sol
                    )));
                }

                let current_exposure = store.live_open_exposure_sol()?;
                if current_exposure + intent.notional_sol > self.risk.max_total_exposure_sol {
                    return Ok(Some(format!(
                        "max_total_exposure_exceeded current={:.6} notional={:.6} max_total={:.6}",
                        current_exposure, intent.notional_sol, self.risk.max_total_exposure_sol
                    )));
                }

                let current_token_exposure =
                    store.live_open_exposure_sol_for_token(&intent.token)?;
                if current_token_exposure + intent.notional_sol
                    > self.risk.max_exposure_per_token_sol
                {
                    return Ok(Some(format!(
                        "max_exposure_per_token_exceeded token={} current={:.6} notional={:.6} max_token={:.6}",
                        intent.token,
                        current_token_exposure,
                        intent.notional_sol,
                        self.risk.max_exposure_per_token_sol
                    )));
                }

                let has_open = store.live_has_open_position(&intent.token)?;
                let open_count = store.live_open_positions_count()?;
                if !has_open && open_count >= self.risk.max_concurrent_positions as u64 {
                    return Ok(Some(format!(
                        "max_concurrent_positions_exceeded open_count={} max={}",
                        open_count, self.risk.max_concurrent_positions
                    )));
                }

                Ok(None)
            }
        }
    }
}

fn build_submit_route_order(
    default_route: &str,
    allowed_routes: &[String],
    configured_order: &[String],
) -> Vec<String> {
    let mut routes = Vec::new();
    let normalized_default = default_route.trim().to_ascii_lowercase();
    if !normalized_default.is_empty() {
        routes.push(normalized_default);
    }
    let mut allowed = Vec::new();
    for route in allowed_routes {
        let normalized = route.trim().to_ascii_lowercase();
        if normalized.is_empty() || allowed.iter().any(|value| value == &normalized) {
            continue;
        }
        allowed.push(normalized);
    }
    for route in configured_order {
        let normalized = route.trim().to_ascii_lowercase();
        if normalized.is_empty() || routes.iter().any(|value| value == &normalized) {
            continue;
        }
        if allowed.iter().any(|value| value == &normalized) {
            routes.push(normalized);
        }
    }
    for route in allowed {
        if !routes.iter().any(|value| value == &route) {
            routes.push(route);
        }
    }
    if routes.is_empty() {
        vec!["paper".to_string()]
    } else {
        routes
    }
}

fn normalize_route(value: &str) -> Option<String> {
    let route = value.trim().to_ascii_lowercase();
    if route.is_empty() {
        None
    } else {
        Some(route)
    }
}

fn normalize_route_tip_lamports(
    route_tip_lamports: &BTreeMap<String, u64>,
) -> BTreeMap<String, u64> {
    route_tip_lamports
        .iter()
        .filter_map(|(route, tip)| normalize_route(route).map(|key| (key, *tip)))
        .collect()
}

fn fee_sol_from_lamports(network_fee_lamports: u64, tip_lamports: u64) -> f64 {
    (network_fee_lamports.saturating_add(tip_lamports) as f64) / 1_000_000_000.0
}

fn fallback_price_and_source(open_position_avg_cost: Option<f64>) -> (f64, String) {
    match open_position_avg_cost {
        Some(avg_cost) => (avg_cost.max(1e-9), "open_position_avg_cost".to_string()),
        None => (1.0, "fixed_1_sol".to_string()),
    }
}

fn bump_route_counter(counters: &mut BTreeMap<String, u64>, route: &str) {
    if route.trim().is_empty() {
        return;
    }
    let entry = counters.entry(route.to_string()).or_insert(0);
    *entry = entry.saturating_add(1);
}

fn accumulate_route_sum(counters: &mut BTreeMap<String, u64>, route: &str, value: u64) {
    if route.trim().is_empty() {
        return;
    }
    let entry = counters.entry(route.to_string()).or_insert(0);
    *entry = entry.saturating_add(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_core_types::SwapEvent;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    struct RetryableFailPreTradeChecker;

    impl PreTradeChecker for RetryableFailPreTradeChecker {
        fn check(
            &self,
            _intent: &ExecutionIntent,
            _route: &str,
        ) -> Result<pretrade::PreTradeDecision> {
            Ok(pretrade::PreTradeDecision::retryable(
                "pretrade_rpc_unavailable",
                "rpc unavailable",
            ))
        }
    }

    struct TerminalRejectPreTradeChecker;

    impl PreTradeChecker for TerminalRejectPreTradeChecker {
        fn check(
            &self,
            _intent: &ExecutionIntent,
            _route: &str,
        ) -> Result<pretrade::PreTradeDecision> {
            Ok(pretrade::PreTradeDecision::reject(
                "pretrade_balance_insufficient",
                "insufficient balance",
            ))
        }
    }

    struct RetryableFailSubmitter;

    impl OrderSubmitter for RetryableFailSubmitter {
        fn submit(
            &self,
            _intent: &ExecutionIntent,
            _client_order_id: &str,
            _route: &str,
        ) -> std::result::Result<submitter::SubmitResult, submitter::SubmitError> {
            Err(submitter::SubmitError::retryable(
                "rpc_timeout",
                "rpc timeout",
            ))
        }
    }

    struct RetryableOnceSubmitter {
        routes: Arc<Mutex<Vec<String>>>,
    }

    impl RetryableOnceSubmitter {
        fn new(routes: Arc<Mutex<Vec<String>>>) -> Self {
            Self { routes }
        }
    }

    impl OrderSubmitter for RetryableOnceSubmitter {
        fn submit(
            &self,
            _intent: &ExecutionIntent,
            _client_order_id: &str,
            route: &str,
        ) -> std::result::Result<submitter::SubmitResult, submitter::SubmitError> {
            let mut calls = self
                .routes
                .lock()
                .expect("retryable submitter routes mutex poisoned");
            calls.push(route.to_string());
            if calls.len() == 1 {
                return Err(submitter::SubmitError::retryable(
                    "submit_retryable_once",
                    "first submit attempt failed",
                ));
            }
            Ok(submitter::SubmitResult {
                route: route.to_string(),
                tx_signature: format!("test-sig-{}", calls.len()),
                submitted_at: Utc::now(),
            })
        }
    }

    struct RetryableOncePreTradeChecker {
        routes: Arc<Mutex<Vec<String>>>,
    }

    impl RetryableOncePreTradeChecker {
        fn new(routes: Arc<Mutex<Vec<String>>>) -> Self {
            Self { routes }
        }
    }

    impl PreTradeChecker for RetryableOncePreTradeChecker {
        fn check(
            &self,
            _intent: &ExecutionIntent,
            route: &str,
        ) -> Result<pretrade::PreTradeDecision> {
            let mut calls = self
                .routes
                .lock()
                .expect("retryable pretrade routes mutex poisoned");
            calls.push(route.to_string());
            if calls.len() == 1 {
                return Ok(pretrade::PreTradeDecision::retryable(
                    "pretrade_retryable_once",
                    "first pretrade attempt failed",
                ));
            }
            Ok(pretrade::PreTradeDecision::allow("ok"))
        }
    }

    struct FailedConfirmer;

    impl OrderConfirmer for FailedConfirmer {
        fn confirm(
            &self,
            _tx_signature: &str,
            _deadline: DateTime<Utc>,
        ) -> Result<confirm::ConfirmationResult> {
            Ok(confirm::ConfirmationResult {
                status: ConfirmationStatus::Failed,
                confirmed_at: None,
                network_fee_lamports: None,
                detail: "forced_failed_confirmation".to_string(),
            })
        }
    }

    struct ErrorConfirmer;

    impl OrderConfirmer for ErrorConfirmer {
        fn confirm(
            &self,
            _tx_signature: &str,
            _deadline: DateTime<Utc>,
        ) -> Result<confirm::ConfirmationResult> {
            Err(anyhow::anyhow!("forced confirmer rpc error"))
        }
    }

    struct NetworkFeeConfirmer {
        network_fee_lamports: u64,
    }

    impl OrderConfirmer for NetworkFeeConfirmer {
        fn confirm(
            &self,
            _tx_signature: &str,
            _deadline: DateTime<Utc>,
        ) -> Result<confirm::ConfirmationResult> {
            Ok(confirm::ConfirmationResult {
                status: ConfirmationStatus::Confirmed,
                confirmed_at: Some(Utc::now()),
                network_fee_lamports: Some(self.network_fee_lamports),
                detail: "forced_confirmed_with_fee".to_string(),
            })
        }
    }

    fn make_test_store(name: &str) -> Result<(SqliteStore, std::path::PathBuf)> {
        let db_path = std::env::temp_dir().join(format!(
            "copybot-exec-{}-{}-{}.db",
            name,
            std::process::id(),
            Utc::now().timestamp_micros()
        ));
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        Ok((store, db_path))
    }

    fn seed_token_price(
        store: &SqliteStore,
        token: &str,
        ts: DateTime<Utc>,
        signature: &str,
    ) -> Result<()> {
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        store.insert_observed_swap(&SwapEvent {
            wallet: "price-feed".to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: token.to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot: 1_000_000,
            ts_utc: ts,
        })?;
        Ok(())
    }

    #[test]
    fn build_submit_route_order_prefers_configured_fallback_sequence() {
        let order = build_submit_route_order(
            "jito",
            &[
                "jito".to_string(),
                "rpc".to_string(),
                "fastlane".to_string(),
            ],
            &["jito".to_string(), "fastlane".to_string()],
        );
        assert_eq!(
            order,
            vec![
                "jito".to_string(),
                "fastlane".to_string(),
                "rpc".to_string()
            ]
        );
    }

    #[test]
    fn build_submit_route_order_ignores_unknown_routes_and_duplicates() {
        let order = build_submit_route_order(
            "jito",
            &["jito".to_string(), "rpc".to_string()],
            &["rpc".to_string(), "unknown".to_string(), "rpc".to_string()],
        );
        assert_eq!(order, vec!["jito".to_string(), "rpc".to_string()]);
    }

    #[test]
    fn fallback_price_and_source_keeps_open_position_label_when_avg_cost_is_exactly_one() {
        let (avg_price, source) = fallback_price_and_source(Some(1.0));
        assert!((avg_price - 1.0).abs() < 1e-12);
        assert_eq!(source, "open_position_avg_cost");

        let (avg_price, source) = fallback_price_and_source(None);
        assert!((avg_price - 1.0).abs() < 1e-12);
        assert_eq!(source, "fixed_1_sol");
    }

    #[test]
    fn process_batch_prioritizes_sell_when_buy_submit_is_paused() -> Result<()> {
        let (store, db_path) = make_test_store("batch-buy-pause-sell-priority")?;
        let now = Utc::now();
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:pause:w:buy:old".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: now - Duration::seconds(10),
            status: "shadow_recorded".to_string(),
        })?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:pause:w:sell:new".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "sell".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "shadow_recorded".to_string(),
        })?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 1.0;
        risk.max_total_exposure_sol = 2.0;
        risk.max_exposure_per_token_sol = 1.0;
        risk.max_concurrent_positions = 10;
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 1;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, now, Some("operator_stop"))?;
        assert_eq!(report.attempted, 1);
        assert_eq!(
            report.skipped, 0,
            "paused BUY must not consume the only batch slot when SELL is pending"
        );
        assert_eq!(
            report.dropped, 1,
            "SELL without open position should be processed and dropped by risk gate"
        );

        let dropped = store.list_copy_signals_by_status("execution_dropped", 10)?;
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].side, "sell");

        let still_shadow = store.list_copy_signals_by_status("shadow_recorded", 10)?;
        assert_eq!(still_shadow.len(), 1);
        assert_eq!(still_shadow[0].side, "buy");

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_skips_paused_pre_submit_buys_across_status_priority() -> Result<()> {
        let (store, db_path) = make_test_store("batch-buy-pause-status-priority")?;
        let now = Utc::now();
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:pause2:w:buy:pending".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: now - Duration::seconds(20),
            status: "execution_pending".to_string(),
        })?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:pause2:w:sell:shadow".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "sell".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: now - Duration::seconds(5),
            status: "shadow_recorded".to_string(),
        })?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 1.0;
        risk.max_total_exposure_sol = 2.0;
        risk.max_exposure_per_token_sol = 1.0;
        risk.max_concurrent_positions = 10;
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 1;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, now, Some("hard_stop"))?;
        assert_eq!(report.attempted, 1);
        assert_eq!(report.skipped, 0);
        assert_eq!(
            report.dropped, 1,
            "SELL from lower-priority status must be processed instead of paused BUY from higher-priority pre-submit status"
        );

        let dropped = store.list_copy_signals_by_status("execution_dropped", 10)?;
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].side, "sell");
        let still_pending = store.list_copy_signals_by_status("execution_pending", 10)?;
        assert_eq!(still_pending.len(), 1);
        assert_eq!(still_pending[0].side, "buy");

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_confirms_shadow_recorded_signal() -> Result<()> {
        let (store, db_path) = make_test_store("batch-confirm")?;
        let now = Utc::now();
        seed_token_price(&store, "token-a", now, "sig-price-1")?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:s1:w:buy:t1".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "shadow_recorded".to_string(),
        })?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 1.0;
        risk.max_total_exposure_sol = 2.0;
        risk.max_concurrent_positions = 10;
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 10;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, Utc::now(), None)?;
        assert_eq!(report.confirmed, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(
            report.confirm_confirmed_by_route.get("paper"),
            Some(&1),
            "confirmed order should be attributed to paper route"
        );
        assert_eq!(
            report.confirm_latency_samples_by_route.get("paper"),
            Some(&1),
            "confirmed order should record one latency sample for paper route"
        );
        assert!(
            report
                .confirm_latency_ms_sum_by_route
                .get("paper")
                .is_some(),
            "confirmed order should record latency sum for paper route"
        );

        let confirmed = store.list_copy_signals_by_status("execution_confirmed", 10)?;
        assert_eq!(confirmed.len(), 1);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_confirms_existing_submitted_order() -> Result<()> {
        let (store, db_path) = make_test_store("batch-confirm-submitted")?;
        let now = Utc::now();
        seed_token_price(&store, "token-a", now, "sig-price-2")?;
        let signal = CopySignalRow {
            signal_id: "shadow:s2:w:sell:t1".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "sell".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        store.insert_copy_signal(&signal)?;
        store.apply_execution_fill_to_positions("token-a", "buy", 1.0, 0.2, now)?;
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-existing-1",
                &signal.signal_id,
                "cb_shadow_s2_w_sell_t1_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted("ord-existing-1", "paper", "paper:tx-existing", now)?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 1.0;
        risk.max_total_exposure_sol = 2.0;
        risk.max_exposure_per_token_sol = 2.0;
        risk.max_concurrent_positions = 10;
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 10;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, Utc::now(), None)?;
        assert_eq!(report.confirmed, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(
            report.confirm_confirmed_by_route.get("paper"),
            Some(&1),
            "confirmed submitted order should be attributed to stored route"
        );
        assert_eq!(
            report.confirm_latency_samples_by_route.get("paper"),
            Some(&1),
            "confirmed submitted order should record one latency sample"
        );
        assert!(
            report
                .confirm_latency_ms_sum_by_route
                .get("paper")
                .is_some(),
            "confirmed submitted order should record latency sum"
        );

        let confirmed = store.list_copy_signals_by_status("execution_confirmed", 10)?;
        assert_eq!(confirmed.len(), 1);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_pauses_buy_submission_when_gate_active() -> Result<()> {
        let (store, db_path) = make_test_store("batch-pause-buy")?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:s3:w:buy:t1".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: Utc::now(),
            status: "shadow_recorded".to_string(),
        })?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 1.0;
        risk.max_total_exposure_sol = 2.0;
        risk.max_exposure_per_token_sol = 2.0;
        risk.max_concurrent_positions = 10;
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 10;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, Utc::now(), Some("hard_stop"))?;
        assert_eq!(report.attempted, 0);
        assert_eq!(report.confirmed, 0);
        assert_eq!(report.skipped, 0);

        let still_shadow = store.list_copy_signals_by_status("shadow_recorded", 10)?;
        assert_eq!(still_shadow.len(), 1);
        let order = store.execution_order_by_client_order_id("cb_shadow_s3_w_buy_t1_a1")?;
        assert!(
            order.is_none(),
            "buy gate should not create execution orders"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_bounds_retryable_submit_failures() -> Result<()> {
        let (store, db_path) = make_test_store("batch-submit-retries")?;
        let signal = CopySignalRow {
            signal_id: "shadow:s4:w:buy:t1".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: Utc::now(),
            status: "shadow_recorded".to_string(),
        };
        store.insert_copy_signal(&signal)?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 100.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        let runtime = ExecutionRuntime {
            enabled: true,
            mode: "paper".to_string(),
            poll_interval_ms: 100,
            batch_size: 10,
            max_confirm_seconds: 15,
            max_submit_attempts: 2,
            max_copy_delay_sec: risk.max_copy_delay_sec.max(1),
            default_route: "paper".to_string(),
            submit_route_order: vec!["paper".to_string()],
            route_tip_lamports: BTreeMap::new(),
            slippage_bps: 50.0,
            simulate_before_submit: true,
            manual_reconcile_required_on_confirm_failure: false,
            risk,
            pretrade: Box::new(PaperPreTradeChecker),
            simulator: Box::new(PaperIntentSimulator),
            submitter: Box::new(RetryableFailSubmitter),
            confirmer: Box::new(PaperOrderConfirmer),
        };

        let first = runtime.process_batch(&store, Utc::now(), None)?;
        assert_eq!(first.failed, 0);
        assert_eq!(first.skipped, 1);

        let client_order_id = idempotency::client_order_id(&signal.signal_id, 1);
        let after_first = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("order should exist after first attempt")?;
        assert_eq!(after_first.status, "execution_simulated");
        assert_eq!(after_first.attempt, 2);

        let second = runtime.process_batch(&store, Utc::now(), None)?;
        assert_eq!(second.failed, 1);
        assert_eq!(
            second.submit_failed_by_route.get("paper"),
            Some(&1),
            "final submit failure should be attributed to active route"
        );

        let failed = store.list_copy_signals_by_status("execution_failed", 10)?;
        assert_eq!(failed.len(), 1);
        let after_second = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("order should remain present after final attempt")?;
        assert_eq!(after_second.status, "execution_failed");
        assert_eq!(after_second.attempt, 2);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_uses_fallback_route_on_submit_retry() -> Result<()> {
        let (store, db_path) = make_test_store("batch-submit-route-fallback")?;
        let now = Utc::now();
        seed_token_price(&store, "token-route-submit", now, "sig-price-route-submit")?;
        let signal = CopySignalRow {
            signal_id: "shadow:s4b:w:buy:route".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-route-submit".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "shadow_recorded".to_string(),
        };
        store.insert_copy_signal(&signal)?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 100.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        let routes = Arc::new(Mutex::new(Vec::new()));
        let runtime = ExecutionRuntime {
            enabled: true,
            mode: "adapter_submit_confirm".to_string(),
            poll_interval_ms: 100,
            batch_size: 10,
            max_confirm_seconds: 15,
            max_submit_attempts: 2,
            max_copy_delay_sec: risk.max_copy_delay_sec.max(1),
            default_route: "jito".to_string(),
            submit_route_order: vec!["jito".to_string(), "rpc".to_string()],
            route_tip_lamports: BTreeMap::new(),
            slippage_bps: 50.0,
            simulate_before_submit: true,
            manual_reconcile_required_on_confirm_failure: true,
            risk,
            pretrade: Box::new(PaperPreTradeChecker),
            simulator: Box::new(PaperIntentSimulator),
            submitter: Box::new(RetryableOnceSubmitter::new(routes.clone())),
            confirmer: Box::new(PaperOrderConfirmer),
        };

        let first = runtime.process_batch(&store, now, None)?;
        assert_eq!(first.failed, 0);
        assert_eq!(first.skipped, 1);
        assert_eq!(
            first.submit_attempted_by_route.get("jito"),
            Some(&1),
            "first attempt should use default route"
        );
        assert_eq!(
            first.submit_retry_scheduled_by_route.get("jito"),
            Some(&1),
            "retry should be scheduled on first route failure"
        );

        let second = runtime.process_batch(&store, Utc::now(), None)?;
        assert_eq!(second.confirmed, 1);
        assert_eq!(
            second.submit_attempted_by_route.get("rpc"),
            Some(&1),
            "second attempt should use fallback route"
        );
        assert_eq!(
            second.confirm_confirmed_by_route.get("rpc"),
            Some(&1),
            "confirmed fallback order should be attributed to rpc route"
        );
        assert_eq!(
            second.confirm_latency_samples_by_route.get("rpc"),
            Some(&1),
            "confirmed fallback order should record one latency sample for rpc route"
        );
        assert!(
            second.confirm_latency_ms_sum_by_route.get("rpc").is_some(),
            "confirmed fallback order should record latency sum for rpc route"
        );
        let observed_routes = routes.lock().expect("routes mutex poisoned").clone();
        assert_eq!(observed_routes, vec!["jito".to_string(), "rpc".to_string()]);

        let order = store
            .execution_order_by_client_order_id("cb_shadow_s4b_w_buy_route_a1")?
            .context("route fallback submit should leave order row")?;
        assert_eq!(order.route, "rpc");
        assert_eq!(order.status, "execution_confirmed");

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_bounds_retryable_pretrade_failures() -> Result<()> {
        let (store, db_path) = make_test_store("batch-pretrade-retries")?;
        let signal = CopySignalRow {
            signal_id: "shadow:s5:w:buy:t1".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: Utc::now(),
            status: "shadow_recorded".to_string(),
        };
        store.insert_copy_signal(&signal)?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 100.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        let runtime = ExecutionRuntime {
            enabled: true,
            mode: "paper".to_string(),
            poll_interval_ms: 100,
            batch_size: 10,
            max_confirm_seconds: 15,
            max_submit_attempts: 2,
            max_copy_delay_sec: risk.max_copy_delay_sec.max(1),
            default_route: "paper".to_string(),
            submit_route_order: vec!["paper".to_string()],
            route_tip_lamports: BTreeMap::new(),
            slippage_bps: 50.0,
            simulate_before_submit: true,
            manual_reconcile_required_on_confirm_failure: false,
            risk,
            pretrade: Box::new(RetryableFailPreTradeChecker),
            simulator: Box::new(PaperIntentSimulator),
            submitter: Box::new(PaperOrderSubmitter),
            confirmer: Box::new(PaperOrderConfirmer),
        };

        let first = runtime.process_batch(&store, Utc::now(), None)?;
        assert_eq!(first.failed, 0);
        assert_eq!(first.skipped, 1);

        let client_order_id = idempotency::client_order_id(&signal.signal_id, 1);
        let after_first = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("order should exist after first pretrade attempt")?;
        assert_eq!(after_first.status, "execution_pending");
        assert_eq!(after_first.attempt, 2);

        let second = runtime.process_batch(&store, Utc::now(), None)?;
        assert_eq!(second.failed, 1);
        assert_eq!(
            second.pretrade_failed_by_route.get("paper"),
            Some(&1),
            "pretrade exhaustion should be attributed to active route"
        );
        let after_second = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("order should remain present after pretrade exhaustion")?;
        assert_eq!(after_second.status, "execution_failed");
        assert_eq!(after_second.attempt, 2);
        assert_eq!(
            after_second.err_code.as_deref(),
            Some("pretrade_attempts_exhausted")
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_drops_terminal_pretrade_rejection() -> Result<()> {
        let (store, db_path) = make_test_store("batch-pretrade-terminal")?;
        let signal = CopySignalRow {
            signal_id: "shadow:s6:w:buy:t1".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: Utc::now(),
            status: "shadow_recorded".to_string(),
        };
        store.insert_copy_signal(&signal)?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 100.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        let runtime = ExecutionRuntime {
            enabled: true,
            mode: "paper".to_string(),
            poll_interval_ms: 100,
            batch_size: 10,
            max_confirm_seconds: 15,
            max_submit_attempts: 2,
            max_copy_delay_sec: risk.max_copy_delay_sec.max(1),
            default_route: "paper".to_string(),
            submit_route_order: vec!["paper".to_string()],
            route_tip_lamports: BTreeMap::new(),
            slippage_bps: 50.0,
            simulate_before_submit: true,
            manual_reconcile_required_on_confirm_failure: false,
            risk,
            pretrade: Box::new(TerminalRejectPreTradeChecker),
            simulator: Box::new(PaperIntentSimulator),
            submitter: Box::new(PaperOrderSubmitter),
            confirmer: Box::new(PaperOrderConfirmer),
        };

        let report = runtime.process_batch(&store, Utc::now(), None)?;
        assert_eq!(report.dropped, 1);
        assert_eq!(
            report.pretrade_terminal_rejected_by_route.get("paper"),
            Some(&1),
            "terminal pretrade reject should be attributed to active route"
        );

        let client_order_id = idempotency::client_order_id(&signal.signal_id, 1);
        let order = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("order should exist for terminal pretrade rejection")?;
        assert_eq!(order.status, "execution_dropped");
        assert_eq!(order.err_code.as_deref(), Some("pretrade_rejected"));
        let dropped = store.list_copy_signals_by_status("execution_dropped", 10)?;
        assert_eq!(dropped.len(), 1);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_uses_fallback_route_on_pretrade_retry() -> Result<()> {
        let (store, db_path) = make_test_store("batch-pretrade-route-fallback")?;
        let now = Utc::now();
        seed_token_price(
            &store,
            "token-route-pretrade",
            now,
            "sig-price-route-pretrade",
        )?;
        let signal = CopySignalRow {
            signal_id: "shadow:s6b:w:buy:route".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-route-pretrade".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "shadow_recorded".to_string(),
        };
        store.insert_copy_signal(&signal)?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 100.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        let routes = Arc::new(Mutex::new(Vec::new()));
        let runtime = ExecutionRuntime {
            enabled: true,
            mode: "adapter_submit_confirm".to_string(),
            poll_interval_ms: 100,
            batch_size: 10,
            max_confirm_seconds: 15,
            max_submit_attempts: 2,
            max_copy_delay_sec: risk.max_copy_delay_sec.max(1),
            default_route: "jito".to_string(),
            submit_route_order: vec!["jito".to_string(), "rpc".to_string()],
            route_tip_lamports: BTreeMap::new(),
            slippage_bps: 50.0,
            simulate_before_submit: true,
            manual_reconcile_required_on_confirm_failure: true,
            risk,
            pretrade: Box::new(RetryableOncePreTradeChecker::new(routes.clone())),
            simulator: Box::new(PaperIntentSimulator),
            submitter: Box::new(PaperOrderSubmitter),
            confirmer: Box::new(PaperOrderConfirmer),
        };

        let first = runtime.process_batch(&store, now, None)?;
        assert_eq!(first.skipped, 1);
        assert_eq!(first.failed, 0);
        assert_eq!(
            first.pretrade_retry_scheduled_by_route.get("jito"),
            Some(&1),
            "first pretrade retry should be attributed to default route"
        );

        let second = runtime.process_batch(&store, Utc::now(), None)?;
        assert_eq!(second.confirmed, 1);
        assert_eq!(
            second.submit_attempted_by_route.get("rpc"),
            Some(&1),
            "second attempt should submit on fallback route after pretrade retry"
        );
        let observed_routes = routes.lock().expect("routes mutex poisoned").clone();
        assert_eq!(observed_routes, vec!["jito".to_string(), "rpc".to_string()]);

        let order = store
            .execution_order_by_client_order_id("cb_shadow_s6b_w_buy_route_a1")?
            .context("route fallback pretrade should leave order row")?;
        assert_eq!(order.route, "rpc");
        assert_eq!(order.attempt, 2);
        assert_eq!(order.status, "execution_confirmed");

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_marks_failed_on_failed_confirmation_status() -> Result<()> {
        let (store, db_path) = make_test_store("batch-confirm-failed-status")?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:s7:w:buy:t1".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: Utc::now(),
            status: "shadow_recorded".to_string(),
        })?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 100.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        let runtime = ExecutionRuntime {
            enabled: true,
            mode: "paper".to_string(),
            poll_interval_ms: 100,
            batch_size: 10,
            max_confirm_seconds: 15,
            max_submit_attempts: 2,
            max_copy_delay_sec: risk.max_copy_delay_sec.max(1),
            default_route: "paper".to_string(),
            submit_route_order: vec!["paper".to_string()],
            route_tip_lamports: BTreeMap::new(),
            slippage_bps: 50.0,
            simulate_before_submit: true,
            manual_reconcile_required_on_confirm_failure: false,
            risk,
            pretrade: Box::new(PaperPreTradeChecker),
            simulator: Box::new(PaperIntentSimulator),
            submitter: Box::new(PaperOrderSubmitter),
            confirmer: Box::new(FailedConfirmer),
        };

        let report = runtime.process_batch(&store, Utc::now(), None)?;
        assert_eq!(report.failed, 1);
        assert_eq!(
            report.confirm_failed_by_route.get("paper"),
            Some(&1),
            "confirm rejected should be attributed to paper route"
        );

        let failed = store.list_copy_signals_by_status("execution_failed", 10)?;
        assert_eq!(failed.len(), 1);
        let order = store
            .execution_order_by_client_order_id("cb_shadow_s7_w_buy_t1_a1")?
            .context("failed confirmation should leave order row")?;
        assert_eq!(order.status, "execution_failed");
        assert_eq!(order.err_code.as_deref(), Some("confirm_rejected"));

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_confirms_with_fallback_when_price_unavailable_on_confirmation() -> Result<()> {
        let (store, db_path) = make_test_store("batch-confirm-price-missing")?;
        let now = Utc::now();
        let signal = CopySignalRow {
            signal_id: "shadow:s8:w:buy:t-missing".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-missing".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        store.insert_copy_signal(&signal)?;

        let client_order_id = idempotency::client_order_id(&signal.signal_id, 1);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-price-missing-1",
                &signal.signal_id,
                &client_order_id,
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-price-missing-1",
            "paper",
            "paper:tx-price-missing",
            now,
        )?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 100.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 10;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.failed, 0);
        assert_eq!(report.confirmed, 1);
        assert_eq!(
            report.confirm_confirmed_by_route.get("paper"),
            Some(&1),
            "missing price fallback path should still confirm and update exposure"
        );
        let exposure = store.live_open_exposure_sol()?;
        assert!(
            (exposure - 0.1).abs() < 1e-9,
            "fallback confirm should keep exposure consistent with notional, got {}",
            exposure
        );

        let confirmed = store.list_copy_signals_by_status("execution_confirmed", 10)?;
        assert_eq!(confirmed.len(), 1);
        let order = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("order should remain present after missing-price fallback confirm")?;
        assert_eq!(order.status, "execution_confirmed");
        assert_eq!(order.err_code.as_deref(), None);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_accounts_network_fee_and_route_tip_in_exposure() -> Result<()> {
        let (store, db_path) = make_test_store("batch-confirm-fee-accounting")?;
        let now = Utc::now();
        seed_token_price(&store, "token-fee", now, "sig-price-fee")?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:s8f:w:buy:t-fee".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-fee".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "shadow_recorded".to_string(),
        })?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 100.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        let mut route_tip_lamports = BTreeMap::new();
        route_tip_lamports.insert("rpc".to_string(), 7000);
        let runtime = ExecutionRuntime {
            enabled: true,
            mode: "adapter_submit_confirm".to_string(),
            poll_interval_ms: 100,
            batch_size: 10,
            max_confirm_seconds: 15,
            max_submit_attempts: 2,
            max_copy_delay_sec: risk.max_copy_delay_sec.max(1),
            default_route: "rpc".to_string(),
            submit_route_order: vec!["rpc".to_string()],
            route_tip_lamports,
            slippage_bps: 50.0,
            simulate_before_submit: true,
            manual_reconcile_required_on_confirm_failure: true,
            risk,
            pretrade: Box::new(PaperPreTradeChecker),
            simulator: Box::new(PaperIntentSimulator),
            submitter: Box::new(PaperOrderSubmitter),
            confirmer: Box::new(NetworkFeeConfirmer {
                network_fee_lamports: 5000,
            }),
        };

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.confirmed, 1);
        let exposure = store.live_open_exposure_sol()?;
        let expected = 0.1 + 12_000.0 / 1_000_000_000.0;
        assert!(
            (exposure - expected).abs() < 1e-12,
            "expected exposure to include network fee + route tip; got {} expected {}",
            exposure,
            expected
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_sell_uses_open_position_avg_cost_when_price_unavailable() -> Result<()> {
        let (store, db_path) = make_test_store("batch-confirm-price-missing-sell")?;
        let now = Utc::now();
        store.apply_execution_fill_to_positions("token-a", "buy", 1.0, 0.25, now)?;
        let signal = CopySignalRow {
            signal_id: "shadow:s8s:w:sell:t-a".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "sell".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.125,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        store.insert_copy_signal(&signal)?;

        let client_order_id = idempotency::client_order_id(&signal.signal_id, 1);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-price-missing-sell-1",
                &signal.signal_id,
                &client_order_id,
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-price-missing-sell-1",
            "paper",
            "paper:tx-price-missing-sell",
            now,
        )?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 100.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 10;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.failed, 0);
        assert_eq!(report.confirmed, 1);
        let token_exposure = store.live_open_exposure_sol_for_token("token-a")?;
        assert!(
            (token_exposure - 0.125).abs() < 1e-9,
            "sell fallback should reduce exposure proportionally using open-position avg cost, got {}",
            token_exposure
        );

        let confirmed = store.list_copy_signals_by_status("execution_confirmed", 10)?;
        assert_eq!(confirmed.len(), 1);
        let order = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("sell order should remain present after fallback confirm")?;
        assert_eq!(order.status, "execution_confirmed");

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_tracks_confirm_retry_by_route_before_deadline() -> Result<()> {
        let (store, db_path) = make_test_store("batch-confirm-retry-route")?;
        let now = Utc::now();
        let signal = CopySignalRow {
            signal_id: "shadow:s8b:w:buy:t-retry".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        store.insert_copy_signal(&signal)?;

        let client_order_id = idempotency::client_order_id(&signal.signal_id, 1);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-confirm-retry-1",
                &signal.signal_id,
                &client_order_id,
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted("ord-confirm-retry-1", "paper", "sig-retry", now)?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 100.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        let runtime = ExecutionRuntime {
            enabled: true,
            mode: "paper".to_string(),
            poll_interval_ms: 100,
            batch_size: 10,
            max_confirm_seconds: 60,
            max_submit_attempts: 2,
            max_copy_delay_sec: risk.max_copy_delay_sec.max(1),
            default_route: "paper".to_string(),
            submit_route_order: vec!["paper".to_string()],
            route_tip_lamports: BTreeMap::new(),
            slippage_bps: 50.0,
            simulate_before_submit: true,
            manual_reconcile_required_on_confirm_failure: false,
            risk,
            pretrade: Box::new(PaperPreTradeChecker),
            simulator: Box::new(PaperIntentSimulator),
            submitter: Box::new(PaperOrderSubmitter),
            confirmer: Box::new(ErrorConfirmer),
        };

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.skipped, 1);
        assert_eq!(
            report.confirm_retry_scheduled_by_route.get("paper"),
            Some(&1),
            "confirm retry should be attributed to paper route"
        );
        assert_eq!(report.confirm_failed_by_route.get("paper"), None);
        let order = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("order should remain in submitted state after retryable confirm error")?;
        assert_eq!(order.status, "execution_submitted");

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_marks_manual_reconcile_required_on_adapter_confirm_error() -> Result<()> {
        let (store, db_path) = make_test_store("batch-confirm-error-manual-reconcile")?;
        let now = Utc::now();
        let submit_ts = now - Duration::seconds(30);
        let signal = CopySignalRow {
            signal_id: "shadow:s9:w:buy:t-adapter".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.1,
            ts: submit_ts,
            status: "execution_submitted".to_string(),
        };
        store.insert_copy_signal(&signal)?;

        let client_order_id = idempotency::client_order_id(&signal.signal_id, 1);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-confirm-error-manual-1",
                &signal.signal_id,
                &client_order_id,
                "rpc",
                submit_ts,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-confirm-error-manual-1",
            "rpc",
            "sig-manual-reconcile",
            submit_ts,
        )?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 100.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        let runtime = ExecutionRuntime {
            enabled: true,
            mode: "adapter_submit_confirm".to_string(),
            poll_interval_ms: 100,
            batch_size: 10,
            max_confirm_seconds: 5,
            max_submit_attempts: 2,
            max_copy_delay_sec: risk.max_copy_delay_sec.max(1),
            default_route: "rpc".to_string(),
            submit_route_order: vec!["rpc".to_string()],
            route_tip_lamports: BTreeMap::new(),
            slippage_bps: 50.0,
            simulate_before_submit: true,
            manual_reconcile_required_on_confirm_failure: true,
            risk,
            pretrade: Box::new(PaperPreTradeChecker),
            simulator: Box::new(PaperIntentSimulator),
            submitter: Box::new(PaperOrderSubmitter),
            confirmer: Box::new(ErrorConfirmer),
        };

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.failed, 1);
        assert_eq!(
            report.confirm_failed_by_route.get("rpc"),
            Some(&1),
            "deadline-passed confirm error should be attributed to rpc route"
        );
        let failed = store.list_copy_signals_by_status("execution_failed", 10)?;
        assert_eq!(failed.len(), 1);
        let order = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("manual reconcile confirm error should leave order row")?;
        assert_eq!(order.status, "execution_failed");
        assert_eq!(
            order.err_code.as_deref(),
            Some("confirm_error_manual_reconcile_required")
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
}
