use crate::batch_report::bump_route_counter;
use crate::intent::ExecutionIntent;
use crate::pretrade::PreTradeDecisionKind;
use crate::submitter::SubmitErrorKind;
use crate::{ExecutionBatchReport, ExecutionRuntime, SignalResult};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_storage::{ExecutionOrderRow, SqliteStore};
use serde_json::json;

impl ExecutionRuntime {
    pub(crate) fn process_pending_order(
        &self,
        store: &SqliteStore,
        intent: &ExecutionIntent,
        client_order_id: &str,
        order: &ExecutionOrderRow,
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
            let sim = self
                .simulator
                .simulate(intent, selected_route)
                .with_context(|| {
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

    pub(crate) fn process_simulated_order(
        &self,
        store: &SqliteStore,
        intent: &ExecutionIntent,
        client_order_id: &str,
        order: &ExecutionOrderRow,
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
                    let next_route = self.submit_route_for_attempt(next_attempt);
                    if !self.submit_fallback_route_allowed(
                        selected_route,
                        next_route,
                        error.code.as_str(),
                    ) {
                        let blocked_detail = format!(
                            "submit_fallback_blocked attempt={} next_attempt={} route={} next_route={} code={} detail={}",
                            attempt,
                            next_attempt,
                            selected_route,
                            next_route,
                            error.code,
                            error.detail
                        );
                        bump_route_counter(&mut report.submit_failed_by_route, selected_route);
                        bump_route_counter(
                            &mut report.submit_fallback_blocked_by_route,
                            selected_route,
                        );
                        store.mark_order_failed(
                            &order.order_id,
                            "submit_fallback_blocked",
                            Some(&blocked_detail),
                        )?;
                        store.update_copy_signal_status(&intent.signal_id, "execution_failed")?;
                        let details = json!({
                            "signal_id": intent.signal_id,
                            "order_id": order.order_id,
                            "attempt": attempt,
                            "next_attempt": next_attempt,
                            "max_submit_attempts": self.max_submit_attempts,
                            "route": selected_route,
                            "next_route": next_route,
                            "error_code": error.code,
                            "retryable": true,
                            "error": error.detail
                        })
                        .to_string();
                        let _ = store.insert_risk_event(
                            "execution_submit_fallback_blocked",
                            "error",
                            now,
                            Some(&details),
                        );
                        return Ok(SignalResult::Failed);
                    }
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
            Some(submit.applied_tip_lamports),
            submit.ata_create_rent_lamports,
            submit.network_fee_lamports_hint,
            submit.base_fee_lamports_hint,
            submit.priority_fee_lamports_hint,
        )?;
        store.update_copy_signal_status(&intent.signal_id, "execution_submitted")?;
        self.process_submitted_order_by_signature(
            store,
            intent,
            &order.order_id,
            submit.route.as_str(),
            submit.tx_signature.as_str(),
            submit.submitted_at,
            Some(submit.applied_tip_lamports),
            submit.ata_create_rent_lamports,
            submit.network_fee_lamports_hint,
            submit.base_fee_lamports_hint,
            submit.priority_fee_lamports_hint,
            now,
            report,
        )
    }

    pub(crate) fn process_submitted_order(
        &self,
        store: &SqliteStore,
        intent: &ExecutionIntent,
        order: &ExecutionOrderRow,
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
            order.applied_tip_lamports,
            order.ata_create_rent_lamports,
            order.network_fee_lamports_hint,
            order.base_fee_lamports_hint,
            order.priority_fee_lamports_hint,
            now,
            report,
        )
    }
}
