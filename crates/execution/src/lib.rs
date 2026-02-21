use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::{ExecutionConfig, RiskConfig};
use copybot_storage::{CopySignalRow, InsertExecutionOrderPendingOutcome, SqliteStore};
use serde_json::json;
use std::collections::HashSet;
use tracing::{debug, warn};
use uuid::Uuid;

pub mod auth;
mod batch_report;
pub mod confirm;
mod confirmation;
pub mod idempotency;
pub mod intent;
mod pipeline;
pub mod pretrade;
pub mod reconcile;
mod runtime;
pub mod simulator;
pub mod submitter;
mod submitter_response;

pub use crate::batch_report::ExecutionBatchReport;
#[cfg(test)]
use confirm::ConfirmationStatus;
use confirm::{FailClosedOrderConfirmer, OrderConfirmer, PaperOrderConfirmer, RpcOrderConfirmer};
use intent::{ExecutionIntent, ExecutionSide};
use pretrade::{
    FailClosedPreTradeChecker, PaperPreTradeChecker, PreTradeChecker, RpcPreTradeChecker,
};
use runtime::{build_submit_route_order, normalize_route_tip_lamports};
use simulator::{
    AdapterIntentSimulator, FailClosedIntentSimulator, IntentSimulator, PaperIntentSimulator,
};
use std::collections::BTreeMap;
use submitter::{
    AdapterOrderSubmitter, FailClosedOrderSubmitter, OrderSubmitter, PaperOrderSubmitter,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignalResult {
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
                let simulator: Box<dyn IntentSimulator + Send + Sync> =
                    match AdapterIntentSimulator::new(
                        &config.submit_adapter_http_url,
                        &config.submit_adapter_fallback_http_url,
                        &config.submit_adapter_auth_token,
                        &config.submit_adapter_hmac_key_id,
                        &config.submit_adapter_hmac_secret,
                        config.submit_adapter_hmac_ttl_sec,
                        &config.submit_adapter_contract_version,
                        config.submit_adapter_require_policy_echo,
                        config.submit_timeout_ms.max(500),
                    ) {
                        Some(value) => Box::new(value),
                        None => Box::new(FailClosedIntentSimulator::new(
                            "adapter simulation init failed for adapter_submit_confirm mode",
                        )),
                    };
                (pretrade, simulator, submitter, confirmer, true)
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

            if let Some(reason) = self.execution_risk_block_reason(store, &intent, now)? {
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
        now: DateTime<Utc>,
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

                let daily_loss_limit_sol = self.daily_loss_limit_sol();
                let max_drawdown_limit_sol = self.max_drawdown_limit_sol();
                let (unrealized_pnl_sol, unrealized_missing_price_count) =
                    if daily_loss_limit_sol.is_some() || max_drawdown_limit_sol.is_some() {
                        store.live_unrealized_pnl_sol(now)?
                    } else {
                        (0.0, 0)
                    };
                if unrealized_missing_price_count > 0 {
                    return Ok(Some(format!(
                        "unrealized_price_unavailable unrealized_missing_price_count={} as_of={}",
                        unrealized_missing_price_count,
                        now.to_rfc3339()
                    )));
                }

                if let Some(daily_loss_limit_sol) = daily_loss_limit_sol {
                    let loss_window_start = now - Duration::hours(24);
                    let (_, realized_pnl_24h) = store.live_realized_pnl_since(loss_window_start)?;
                    let pnl_24h = realized_pnl_24h + unrealized_pnl_sol;
                    if pnl_24h <= -daily_loss_limit_sol {
                        return Ok(Some(format!(
                            "daily_loss_limit_exceeded pnl_24h={:.6} realized_pnl_24h={:.6} unrealized_pnl={:.6} unrealized_missing_price_count={} loss_limit_sol={:.6} loss_limit_pct={:.4} window_start={}",
                            pnl_24h,
                            realized_pnl_24h,
                            unrealized_pnl_sol,
                            unrealized_missing_price_count,
                            daily_loss_limit_sol,
                            self.risk.daily_loss_limit_pct,
                            loss_window_start.to_rfc3339()
                        )));
                    }
                }

                if let Some(max_drawdown_limit_sol) = max_drawdown_limit_sol {
                    let drawdown_window_start = now - Duration::hours(24);
                    let max_drawdown_sol = store.live_max_drawdown_with_unrealized_since(
                        drawdown_window_start,
                        unrealized_pnl_sol,
                    )?;
                    if max_drawdown_sol >= max_drawdown_limit_sol {
                        return Ok(Some(format!(
                            "max_drawdown_exceeded max_drawdown_sol={:.6} unrealized_missing_price_count={} drawdown_limit_sol={:.6} drawdown_limit_pct={:.4} window_start={}",
                            max_drawdown_sol,
                            unrealized_missing_price_count,
                            max_drawdown_limit_sol,
                            self.risk.max_drawdown_pct,
                            drawdown_window_start.to_rfc3339()
                        )));
                    }
                }

                Ok(None)
            }
        }
    }

    fn daily_loss_limit_sol(&self) -> Option<f64> {
        if !self.risk.daily_loss_limit_pct.is_finite() || self.risk.daily_loss_limit_pct <= 0.0 {
            return None;
        }
        Some(self.risk.max_total_exposure_sol * (self.risk.daily_loss_limit_pct / 100.0))
    }

    fn max_drawdown_limit_sol(&self) -> Option<f64> {
        if !self.risk.max_drawdown_pct.is_finite() || self.risk.max_drawdown_pct <= 0.0 {
            return None;
        }
        Some(self.risk.max_total_exposure_sol * (self.risk.max_drawdown_pct / 100.0))
    }
}

pub(crate) fn fee_sol_from_lamports(
    network_fee_lamports: u64,
    tip_lamports: u64,
    ata_create_rent_lamports: u64,
) -> f64 {
    (network_fee_lamports
        .saturating_add(tip_lamports)
        .saturating_add(ata_create_rent_lamports) as f64)
        / 1_000_000_000.0
}

pub(crate) fn fallback_price_and_source(
    open_position_avg_cost: Option<f64>,
) -> Option<(f64, String)> {
    open_position_avg_cost
        .map(|avg_cost| (avg_cost.max(1e-9), "open_position_avg_cost".to_string()))
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
                applied_tip_lamports: 0,
                ata_create_rent_lamports: None,
                network_fee_lamports_hint: None,
                base_fee_lamports_hint: None,
                priority_fee_lamports_hint: None,
            })
        }
    }

    struct FixedTipSubmitter {
        tip_lamports: u64,
    }

    impl OrderSubmitter for FixedTipSubmitter {
        fn submit(
            &self,
            _intent: &ExecutionIntent,
            _client_order_id: &str,
            route: &str,
        ) -> std::result::Result<submitter::SubmitResult, submitter::SubmitError> {
            Ok(submitter::SubmitResult {
                route: route.to_string(),
                tx_signature: "fixed-tip-sig".to_string(),
                submitted_at: Utc::now(),
                applied_tip_lamports: self.tip_lamports,
                ata_create_rent_lamports: None,
                network_fee_lamports_hint: None,
                base_fee_lamports_hint: None,
                priority_fee_lamports_hint: None,
            })
        }
    }

    struct FixedFeeHintSubmitter {
        tip_lamports: u64,
        network_fee_lamports_hint: u64,
    }

    impl OrderSubmitter for FixedFeeHintSubmitter {
        fn submit(
            &self,
            _intent: &ExecutionIntent,
            _client_order_id: &str,
            route: &str,
        ) -> std::result::Result<submitter::SubmitResult, submitter::SubmitError> {
            Ok(submitter::SubmitResult {
                route: route.to_string(),
                tx_signature: "fixed-fee-hint-sig".to_string(),
                submitted_at: Utc::now(),
                applied_tip_lamports: self.tip_lamports,
                ata_create_rent_lamports: None,
                network_fee_lamports_hint: Some(self.network_fee_lamports_hint),
                base_fee_lamports_hint: None,
                priority_fee_lamports_hint: None,
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
                network_fee_lookup_error: None,
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
                network_fee_lookup_error: None,
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
        let (avg_price, source) = fallback_price_and_source(Some(1.0)).expect("fallback exists");
        assert!((avg_price - 1.0).abs() < 1e-12);
        assert_eq!(source, "open_position_avg_cost");

        assert!(
            fallback_price_and_source(None).is_none(),
            "fallback should be unavailable when open position avg cost is missing"
        );
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
        store.mark_order_submitted(
            "ord-existing-1",
            "paper",
            "paper:tx-existing",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;

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
    fn process_batch_drops_buy_when_daily_loss_limit_is_exceeded() -> Result<()> {
        let (store, db_path) = make_test_store("batch-daily-loss-limit")?;
        let now = Utc::now();
        store.apply_execution_fill_to_positions(
            "token-loss-seed",
            "buy",
            1.0,
            0.20,
            now - Duration::minutes(90),
        )?;
        store.apply_execution_fill_to_positions(
            "token-loss-seed",
            "sell",
            1.0,
            0.10,
            now - Duration::minutes(60),
        )?;

        let signal = CopySignalRow {
            signal_id: "shadow:s7:w:buy:daily".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-buy-daily".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "execution_pending".to_string(),
        };
        store.insert_copy_signal(&signal)?;
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-daily-loss-1",
                &signal.signal_id,
                "cb_shadow_s7_w_buy_daily_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 1.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        risk.daily_loss_limit_pct = 5.0; // 0.05 SOL on 1.0 exposure budget.
        risk.max_drawdown_pct = 100.0; // keep drawdown gate out of this test.
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 10;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.dropped, 1);

        let order = store
            .execution_order_by_client_order_id("cb_shadow_s7_w_buy_daily_a1")?
            .context("daily-loss blocked signal should create and drop execution order")?;
        assert_eq!(order.status, "execution_dropped");
        assert_eq!(order.err_code.as_deref(), Some("risk_blocked"));
        assert!(
            order
                .simulation_error
                .as_deref()
                .unwrap_or_default()
                .contains("daily_loss_limit_exceeded"),
            "unexpected risk block detail: {:?}",
            order.simulation_error
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_drops_buy_when_max_drawdown_is_exceeded() -> Result<()> {
        let (store, db_path) = make_test_store("batch-max-drawdown-limit")?;
        let now = Utc::now();
        store.apply_execution_fill_to_positions(
            "token-drawdown-seed",
            "buy",
            1.0,
            0.10,
            now - Duration::hours(3),
        )?;
        store.apply_execution_fill_to_positions(
            "token-drawdown-seed",
            "sell",
            1.0,
            0.16,
            now - Duration::minutes(170),
        )?;
        store.apply_execution_fill_to_positions(
            "token-drawdown-seed",
            "buy",
            1.0,
            0.20,
            now - Duration::minutes(160),
        )?;
        store.apply_execution_fill_to_positions(
            "token-drawdown-seed",
            "sell",
            1.0,
            0.05,
            now - Duration::minutes(150),
        )?;

        let signal = CopySignalRow {
            signal_id: "shadow:s8:w:buy:drawdown".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-buy-drawdown".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "execution_pending".to_string(),
        };
        store.insert_copy_signal(&signal)?;
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-max-drawdown-1",
                &signal.signal_id,
                "cb_shadow_s8_w_buy_drawdown_a1",
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 1.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        risk.daily_loss_limit_pct = 0.0; // isolate drawdown gate in this test.
        risk.max_drawdown_pct = 5.0; // 0.05 SOL on 1.0 exposure budget.
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 10;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.dropped, 1);

        let order = store
            .execution_order_by_client_order_id("cb_shadow_s8_w_buy_drawdown_a1")?
            .context("drawdown-blocked signal should create and drop execution order")?;
        assert_eq!(order.status, "execution_dropped");
        assert_eq!(order.err_code.as_deref(), Some("risk_blocked"));
        assert!(
            order
                .simulation_error
                .as_deref()
                .unwrap_or_default()
                .contains("max_drawdown_exceeded"),
            "unexpected risk block detail: {:?}",
            order.simulation_error
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_drops_buy_when_daily_loss_is_exceeded_by_unrealized_pnl() -> Result<()> {
        let (store, db_path) = make_test_store("batch-daily-loss-unrealized")?;
        let now = Utc::now();
        store.apply_execution_fill_to_positions(
            "token-open-loss",
            "buy",
            1.0,
            0.20,
            now - Duration::minutes(10),
        )?;
        seed_token_price(
            &store,
            "token-open-loss",
            now - Duration::minutes(9),
            "sig-price-open-loss",
        )?;

        let signal = CopySignalRow {
            signal_id: "shadow:s10:w:buy:daily-unrealized".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-daily-unrealized".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "execution_pending".to_string(),
        };
        store.insert_copy_signal(&signal)?;
        let client_order_id = idempotency::client_order_id(&signal.signal_id, 1);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-daily-loss-unrealized-1",
                &signal.signal_id,
                &client_order_id,
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 1.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        risk.daily_loss_limit_pct = 5.0; // 0.05 SOL
        risk.max_drawdown_pct = 100.0;
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 10;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.dropped, 1);

        let order = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("daily-loss-unrealized blocked signal should create and drop order")?;
        assert_eq!(order.status, "execution_dropped");
        assert_eq!(order.err_code.as_deref(), Some("risk_blocked"));
        let detail = order.simulation_error.unwrap_or_default();
        assert!(
            detail.contains("daily_loss_limit_exceeded") && detail.contains("unrealized_pnl"),
            "unexpected risk block detail: {detail}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_drops_buy_when_drawdown_is_exceeded_by_unrealized_pnl() -> Result<()> {
        let (store, db_path) = make_test_store("batch-drawdown-unrealized")?;
        let now = Utc::now();
        store.apply_execution_fill_to_positions(
            "token-closed-profit",
            "buy",
            1.0,
            0.10,
            now - Duration::minutes(20),
        )?;
        store.apply_execution_fill_to_positions(
            "token-closed-profit",
            "sell",
            1.0,
            0.30,
            now - Duration::minutes(19),
        )?;
        store.apply_execution_fill_to_positions(
            "token-open-drawdown",
            "buy",
            1.0,
            0.40,
            now - Duration::minutes(10),
        )?;
        seed_token_price(
            &store,
            "token-open-drawdown",
            now - Duration::minutes(9),
            "sig-price-open-drawdown",
        )?;

        let signal = CopySignalRow {
            signal_id: "shadow:s11:w:buy:drawdown-unrealized".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-drawdown-unrealized".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "execution_pending".to_string(),
        };
        store.insert_copy_signal(&signal)?;
        let client_order_id = idempotency::client_order_id(&signal.signal_id, 1);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-drawdown-unrealized-1",
                &signal.signal_id,
                &client_order_id,
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 1.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        risk.daily_loss_limit_pct = 0.0;
        risk.max_drawdown_pct = 5.0; // 0.05 SOL
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 10;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.dropped, 1);

        let order = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("drawdown-unrealized blocked signal should create and drop order")?;
        assert_eq!(order.status, "execution_dropped");
        assert_eq!(order.err_code.as_deref(), Some("risk_blocked"));
        let detail = order.simulation_error.unwrap_or_default();
        assert!(
            detail.contains("max_drawdown_exceeded"),
            "unexpected risk block detail: {detail}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_drops_buy_when_unrealized_price_is_missing() -> Result<()> {
        let (store, db_path) = make_test_store("batch-unrealized-price-missing")?;
        let now = Utc::now();
        store.apply_execution_fill_to_positions(
            "token-open-no-quote",
            "buy",
            1.0,
            0.20,
            now - Duration::minutes(10),
        )?;

        let signal = CopySignalRow {
            signal_id: "shadow:s12:w:buy:missing-price".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-buy-missing-price".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "execution_pending".to_string(),
        };
        store.insert_copy_signal(&signal)?;
        let client_order_id = idempotency::client_order_id(&signal.signal_id, 1);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-missing-price-1",
                &signal.signal_id,
                &client_order_id,
                "paper",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 1.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        risk.daily_loss_limit_pct = 5.0;
        risk.max_drawdown_pct = 5.0;
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 10;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.dropped, 1);

        let order = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("missing-price blocked signal should create and drop order")?;
        assert_eq!(order.status, "execution_dropped");
        assert_eq!(order.err_code.as_deref(), Some("risk_blocked"));
        let detail = order.simulation_error.unwrap_or_default();
        assert!(
            detail.contains("unrealized_price_unavailable"),
            "unexpected risk block detail: {detail}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_allows_buy_when_drawdown_breach_is_outside_24h_window() -> Result<()> {
        let (store, db_path) = make_test_store("batch-max-drawdown-window-clears")?;
        let now = Utc::now();
        let old = now - Duration::hours(30);
        store.apply_execution_fill_to_positions("token-window-seed", "buy", 1.0, 0.10, old)?;
        store.apply_execution_fill_to_positions(
            "token-window-seed",
            "sell",
            1.0,
            0.20,
            old + Duration::minutes(1),
        )?;
        store.apply_execution_fill_to_positions(
            "token-window-seed",
            "buy",
            1.0,
            0.20,
            old + Duration::minutes(2),
        )?;
        store.apply_execution_fill_to_positions(
            "token-window-seed",
            "sell",
            1.0,
            0.05,
            old + Duration::minutes(3),
        )?;
        seed_token_price(&store, "token-window-new", now, "sig-price-window-new")?;

        let signal = CopySignalRow {
            signal_id: "shadow:s9:w:buy:drawdown-clear".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-window-new".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "shadow_recorded".to_string(),
        };
        store.insert_copy_signal(&signal)?;

        let mut risk = RiskConfig::default();
        risk.max_position_sol = 10.0;
        risk.max_total_exposure_sol = 1.0;
        risk.max_exposure_per_token_sol = 10.0;
        risk.max_concurrent_positions = 100;
        risk.daily_loss_limit_pct = 0.0;
        risk.max_drawdown_pct = 5.0; // 0.05 SOL threshold.
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        execution.batch_size = 10;
        execution.mode = "paper".to_string();
        let runtime = ExecutionRuntime::from_config(execution, risk);

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(
            report.confirmed, 1,
            "old drawdown events outside 24h window should not block new BUY"
        );
        assert_eq!(report.dropped, 0);

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
    fn process_batch_fails_confirm_when_price_unavailable_without_fallback() -> Result<()> {
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
            None,
            None,
            None,
            None,
            None,
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
        assert_eq!(report.failed, 1);
        assert_eq!(report.confirmed, 0);
        let exposure = store.live_open_exposure_sol()?;
        assert!(
            exposure.abs() < 1e-9,
            "failed confirm without fallback must not write synthetic exposure, got {}",
            exposure
        );

        let failed = store.list_copy_signals_by_status("execution_failed", 10)?;
        assert_eq!(failed.len(), 1);
        let order = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("order should remain present after missing-price confirm failure")?;
        assert_eq!(order.status, "execution_failed");
        assert_eq!(order.err_code.as_deref(), Some("confirm_price_unavailable"));

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_marks_manual_reconcile_when_price_unavailable_without_fallback() -> Result<()>
    {
        let (store, db_path) = make_test_store("batch-confirm-price-missing-manual-reconcile")?;
        let now = Utc::now();
        let signal = CopySignalRow {
            signal_id: "shadow:s8:w:buy:t-missing-manual".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-missing-manual".to_string(),
            notional_sol: 0.1,
            ts: now,
            status: "execution_submitted".to_string(),
        };
        store.insert_copy_signal(&signal)?;

        let client_order_id = idempotency::client_order_id(&signal.signal_id, 1);
        assert_eq!(
            store.insert_execution_order_pending(
                "ord-price-missing-manual-1",
                &signal.signal_id,
                &client_order_id,
                "rpc",
                now,
                1
            )?,
            InsertExecutionOrderPendingOutcome::Inserted
        );
        store.mark_order_submitted(
            "ord-price-missing-manual-1",
            "rpc",
            "sig-missing-price-manual",
            now,
            None,
            None,
            None,
            None,
            None,
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
            max_confirm_seconds: 15,
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
            confirmer: Box::new(PaperOrderConfirmer),
        };

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.failed, 1);
        assert_eq!(report.confirmed, 0);
        let order = store
            .execution_order_by_client_order_id(&client_order_id)?
            .context("order should remain present after missing-price manual-reconcile failure")?;
        assert_eq!(order.status, "execution_failed");
        assert_eq!(
            order.err_code.as_deref(),
            Some("confirm_price_unavailable_manual_reconcile_required")
        );

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
            submitter: Box::new(FixedTipSubmitter { tip_lamports: 7000 }),
            confirmer: Box::new(NetworkFeeConfirmer {
                network_fee_lamports: 5000,
            }),
        };

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.confirmed, 1);
        assert_eq!(report.confirm_network_fee_rpc_by_route.get("rpc"), Some(&1));
        assert_eq!(
            report.confirm_network_fee_submit_hint_by_route.get("rpc"),
            None
        );
        assert_eq!(report.confirm_network_fee_missing_by_route.get("rpc"), None);
        assert_eq!(
            report.confirm_network_fee_lamports_sum_by_route.get("rpc"),
            Some(&5_000)
        );
        assert_eq!(
            report.confirm_tip_lamports_sum_by_route.get("rpc"),
            Some(&7_000)
        );
        assert_eq!(
            report.confirm_ata_rent_lamports_sum_by_route.get("rpc"),
            Some(&0)
        );
        assert_eq!(
            report.confirm_fee_total_lamports_sum_by_route.get("rpc"),
            Some(&12_000)
        );
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
    fn process_batch_uses_submit_network_fee_hint_when_rpc_fee_unavailable() -> Result<()> {
        let (store, db_path) = make_test_store("batch-confirm-fee-hint-fallback")?;
        let now = Utc::now();
        seed_token_price(&store, "token-fee-hint", now, "sig-price-fee-hint")?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:s8g:w:buy:t-fee-hint".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-fee-hint".to_string(),
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
        route_tip_lamports.insert("rpc".to_string(), 0);
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
            submitter: Box::new(FixedFeeHintSubmitter {
                tip_lamports: 0,
                network_fee_lamports_hint: 1_000_000_000,
            }),
            confirmer: Box::new(PaperOrderConfirmer),
        };

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.confirmed, 1);
        assert_eq!(
            report.confirm_network_fee_submit_hint_by_route.get("rpc"),
            Some(&1)
        );
        assert_eq!(report.confirm_network_fee_rpc_by_route.get("rpc"), None);
        assert_eq!(report.confirm_network_fee_missing_by_route.get("rpc"), None);
        assert_eq!(
            report.confirm_network_fee_lamports_sum_by_route.get("rpc"),
            Some(&1_000_000_000)
        );
        assert_eq!(
            report.confirm_tip_lamports_sum_by_route.get("rpc"),
            Some(&0)
        );
        assert_eq!(
            report.confirm_ata_rent_lamports_sum_by_route.get("rpc"),
            Some(&0)
        );
        assert_eq!(
            report.confirm_fee_total_lamports_sum_by_route.get("rpc"),
            Some(&1_000_000_000)
        );
        let exposure = store.live_open_exposure_sol()?;
        let expected = 1.1;
        assert!(
            (exposure - expected).abs() < 1e-12,
            "expected exposure to include submit network fee hint fallback; got {} expected {}",
            exposure,
            expected
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn process_batch_prefers_rpc_network_fee_over_submit_hint() -> Result<()> {
        let (store, db_path) = make_test_store("batch-confirm-fee-rpc-priority")?;
        let now = Utc::now();
        seed_token_price(
            &store,
            "token-fee-rpc-priority",
            now,
            "sig-price-fee-rpc-priority",
        )?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:s8h:w:buy:t-fee-rpc-priority".to_string(),
            wallet_id: "wallet-a".to_string(),
            side: "buy".to_string(),
            token: "token-fee-rpc-priority".to_string(),
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
        route_tip_lamports.insert("rpc".to_string(), 0);
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
            submitter: Box::new(FixedFeeHintSubmitter {
                tip_lamports: 0,
                network_fee_lamports_hint: 1_000_000_000,
            }),
            confirmer: Box::new(NetworkFeeConfirmer {
                network_fee_lamports: 5_000,
            }),
        };

        let report = runtime.process_batch(&store, now, None)?;
        assert_eq!(report.confirmed, 1);
        assert_eq!(report.confirm_network_fee_rpc_by_route.get("rpc"), Some(&1));
        assert_eq!(
            report.confirm_network_fee_submit_hint_by_route.get("rpc"),
            None
        );
        assert_eq!(report.confirm_network_fee_missing_by_route.get("rpc"), None);
        assert_eq!(
            report.confirm_network_fee_lamports_sum_by_route.get("rpc"),
            Some(&5_000)
        );
        assert_eq!(
            report.confirm_tip_lamports_sum_by_route.get("rpc"),
            Some(&0)
        );
        assert_eq!(
            report.confirm_ata_rent_lamports_sum_by_route.get("rpc"),
            Some(&0)
        );
        assert_eq!(
            report.confirm_fee_total_lamports_sum_by_route.get("rpc"),
            Some(&5_000)
        );
        let exposure = store.live_open_exposure_sol()?;
        let expected = 0.1 + 5_000.0 / 1_000_000_000.0;
        assert!(
            (exposure - expected).abs() < 1e-12,
            "expected exposure to prefer rpc network fee over submit hint; got {} expected {}",
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
            None,
            None,
            None,
            None,
            None,
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
        store.mark_order_submitted(
            "ord-confirm-retry-1",
            "paper",
            "sig-retry",
            now,
            None,
            None,
            None,
            None,
            None,
        )?;

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
            None,
            None,
            None,
            None,
            None,
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
