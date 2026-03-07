use crate::batch_report::{accumulate_route_sum, bump_route_counter};
use crate::confirm::ConfirmationStatus;
use crate::intent::ExecutionIntent;
use crate::reconcile::{build_fill_from_confirmed_observation, build_fill_from_priced_intent};
use crate::{
    fallback_price_and_source, fee_sol_from_lamports, ExecutionBatchReport, ExecutionRuntime,
    SignalResult,
};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage::{
    FinalizeExecutionConfirmOutcome, SqliteStore, EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
};
use serde_json::json;
use tracing::warn;

struct SyntheticPriceResolution {
    avg_price_sol: f64,
    used_price_fallback: bool,
    fallback_source: Option<String>,
}

impl ExecutionRuntime {
    #[allow(clippy::too_many_arguments)]
    fn skip_confirm_manual_reconcile(
        &self,
        store: &SqliteStore,
        intent: &ExecutionIntent,
        order_id: &str,
        lifecycle_status: &str,
        route: &str,
        now: DateTime<Utc>,
        report: &mut ExecutionBatchReport,
        err_code: &str,
        risk_event_type: &str,
        details: serde_json::Value,
    ) -> Result<SignalResult> {
        if lifecycle_status != EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS {
            store.mark_order_reconcile_pending(order_id, err_code)?;
            store.update_copy_signal_status(
                &intent.signal_id,
                EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
            )?;
            let _ = store.insert_risk_event(
                risk_event_type,
                "error",
                now,
                Some(&details.to_string()),
            );
        }
        bump_route_counter(&mut report.confirm_retry_scheduled_by_route, route);
        Ok(SignalResult::Skipped)
    }

    fn live_manual_reconcile_fail_closed_on_confirm_failure(&self) -> bool {
        self.mode == "adapter_submit_confirm" && self.manual_reconcile_required_on_confirm_failure
    }

    fn resolve_synthetic_price(
        &self,
        store: &SqliteStore,
        intent: &ExecutionIntent,
        route: &str,
        confirmed_at: DateTime<Utc>,
    ) -> Result<Option<SyntheticPriceResolution>> {
        match store.latest_token_sol_price(&intent.token, confirmed_at)? {
            Some(price) if price.is_finite() && price > 0.0 => Ok(Some(SyntheticPriceResolution {
                avg_price_sol: price.max(1e-9),
                used_price_fallback: false,
                fallback_source: None,
            })),
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
                let Some((fallback, source)) = fallback_price_and_source(open_position_avg_cost) else {
                    return Ok(None);
                };
                warn!(
                    signal_id = %intent.signal_id,
                    token = %intent.token,
                    route,
                    fallback_avg_price_sol = fallback,
                    fallback_source = %source,
                    "latest token price unavailable; using fallback price to keep confirmed reconcile/exposure consistent"
                );
                Ok(Some(SyntheticPriceResolution {
                    avg_price_sol: fallback,
                    used_price_fallback: true,
                    fallback_source: Some(source),
                }))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn process_submitted_order_by_signature(
        &self,
        store: &SqliteStore,
        intent: &ExecutionIntent,
        order_id: &str,
        route: &str,
        tx_signature: &str,
        lifecycle_status: &str,
        submit_ts: DateTime<Utc>,
        applied_tip_lamports: Option<u64>,
        ata_create_rent_lamports: Option<u64>,
        network_fee_lamports_hint: Option<u64>,
        base_fee_lamports_hint: Option<u64>,
        priority_fee_lamports_hint: Option<u64>,
        now: DateTime<Utc>,
        report: &mut ExecutionBatchReport,
    ) -> Result<SignalResult> {
        let deadline = submit_ts + Duration::seconds(self.max_confirm_seconds as i64);
        let confirm = match self
            .confirmer
            .confirm(tx_signature, &intent.token, deadline)
        {
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
                let manual_reconcile_required =
                    self.live_manual_reconcile_fail_closed_on_confirm_failure();
                let err_code = if manual_reconcile_required {
                    "confirm_error_manual_reconcile_required"
                } else {
                    "confirm_error"
                };
                let detail = format!(
                    "confirm_error deadline_passed mode={} manual_reconcile_required={} error={}",
                    self.mode, manual_reconcile_required, error
                );
                if manual_reconcile_required {
                    return self.skip_confirm_manual_reconcile(
                        store,
                        intent,
                        order_id,
                        lifecycle_status,
                        route,
                        now,
                        report,
                        err_code,
                        "execution_confirm_failed_manual_reconcile_required",
                        json!({
                            "signal_id": intent.signal_id,
                            "order_id": order_id,
                            "mode": self.mode,
                            "manual_reconcile_required": true,
                            "deadline": deadline.to_rfc3339(),
                            "error": error.to_string(),
                        }),
                    );
                }
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
                let route_tip_lamports =
                    applied_tip_lamports.unwrap_or_else(|| self.route_tip_lamports(route));
                let ata_create_rent_lamports = ata_create_rent_lamports.unwrap_or(0);
                let resolved_network_fee_lamports = confirm
                    .network_fee_lamports
                    .or(network_fee_lamports_hint)
                    .unwrap_or(0);
                let resolved_total_fee_lamports = resolved_network_fee_lamports
                    .saturating_add(route_tip_lamports)
                    .saturating_add(ata_create_rent_lamports);
                if self.mode == "adapter_submit_confirm" {
                    if confirm.network_fee_lamports.is_some() {
                        bump_route_counter(&mut report.confirm_network_fee_rpc_by_route, route);
                    } else if network_fee_lamports_hint.is_some() {
                        bump_route_counter(
                            &mut report.confirm_network_fee_submit_hint_by_route,
                            route,
                        );
                    } else {
                        bump_route_counter(&mut report.confirm_network_fee_missing_by_route, route);
                    }
                    if let (
                        Some(rpc_network_fee_lamports),
                        Some(submit_network_fee_lamports_hint),
                    ) = (confirm.network_fee_lamports, network_fee_lamports_hint)
                    {
                        if rpc_network_fee_lamports != submit_network_fee_lamports_hint {
                            let details = json!({
                                "signal_id": intent.signal_id,
                                "order_id": order_id,
                                "route": route,
                                "rpc_network_fee_lamports": rpc_network_fee_lamports,
                                "submit_network_fee_lamports_hint": submit_network_fee_lamports_hint,
                                "base_fee_lamports_hint": base_fee_lamports_hint,
                                "priority_fee_lamports_hint": priority_fee_lamports_hint,
                                "absolute_diff_lamports": rpc_network_fee_lamports.abs_diff(submit_network_fee_lamports_hint),
                                "reason": "rpc_network_fee_differs_from_submit_hint",
                            })
                            .to_string();
                            let _ = store.insert_risk_event(
                                "execution_network_fee_hint_mismatch",
                                "warn",
                                now,
                                Some(&details),
                            );
                        }
                    }
                }
                accumulate_route_sum(
                    &mut report.confirm_network_fee_lamports_sum_by_route,
                    route,
                    resolved_network_fee_lamports,
                );
                accumulate_route_sum(
                    &mut report.confirm_tip_lamports_sum_by_route,
                    route,
                    route_tip_lamports,
                );
                accumulate_route_sum(
                    &mut report.confirm_ata_rent_lamports_sum_by_route,
                    route,
                    ata_create_rent_lamports,
                );
                accumulate_route_sum(
                    &mut report.confirm_fee_total_lamports_sum_by_route,
                    route,
                    resolved_total_fee_lamports,
                );
                if let Some(base_fee_lamports_hint) = base_fee_lamports_hint {
                    accumulate_route_sum(
                        &mut report.confirm_base_fee_hint_lamports_sum_by_route,
                        route,
                        base_fee_lamports_hint,
                    );
                }
                if let Some(priority_fee_lamports_hint) = priority_fee_lamports_hint {
                    accumulate_route_sum(
                        &mut report.confirm_priority_fee_hint_lamports_sum_by_route,
                        route,
                        priority_fee_lamports_hint,
                    );
                }
                let execution_fee_sol = fee_sol_from_lamports(
                    resolved_network_fee_lamports,
                    route_tip_lamports,
                    ata_create_rent_lamports,
                );
                let live_manual_reconcile_fail_closed =
                    self.live_manual_reconcile_fail_closed_on_confirm_failure();
                let build_synthetic_fill =
                    |resolution: SyntheticPriceResolution| -> Result<(crate::reconcile::ExecutionFill, bool, Option<String>)> {
                        Ok((
                            build_fill_from_priced_intent(
                                intent,
                                order_id,
                                resolution.avg_price_sol,
                                self.slippage_bps,
                                execution_fee_sol,
                            )?,
                            resolution.used_price_fallback,
                            resolution.fallback_source,
                        ))
                    };
                let mut fail_confirm_price_unavailable = || -> Result<SignalResult> {
                    let manual_reconcile_required =
                        self.manual_reconcile_required_on_confirm_failure;
                    let err_code = if manual_reconcile_required {
                        "confirm_price_unavailable_manual_reconcile_required"
                    } else {
                        "confirm_price_unavailable"
                    };
                    let detail = "price_unavailable_no_position_avg";
                    store.mark_order_failed(order_id, err_code, Some(detail))?;
                    store.update_copy_signal_status(&intent.signal_id, "execution_failed")?;
                    bump_route_counter(&mut report.confirm_failed_by_route, route);

                    let details = json!({
                        "signal_id": intent.signal_id,
                        "order_id": order_id,
                        "token": intent.token,
                        "route": route,
                        "err_code": err_code,
                        "reason": "missing_latest_price_no_fallback",
                        "manual_reconcile_required": manual_reconcile_required,
                    })
                    .to_string();
                    let _ = store.insert_risk_event(
                        if manual_reconcile_required {
                            "execution_confirm_price_unavailable_manual_reconcile_required"
                        } else {
                            "execution_confirm_price_unavailable"
                        },
                        "error",
                        now,
                        Some(&details),
                    );
                    Ok(SignalResult::Failed)
                };

                let (fill, used_price_fallback, fallback_source) =
                    if let Some(observed_fill) = confirm.observed_fill {
                    match build_fill_from_confirmed_observation(
                        intent,
                        order_id,
                        observed_fill,
                        self.slippage_bps,
                        execution_fee_sol,
                    ) {
                        Ok(fill) => (Some(fill), false, None),
                        Err(error) => {
                            let synthetic_price =
                                self.resolve_synthetic_price(store, intent, route, confirmed_at)?;
                            if live_manual_reconcile_fail_closed {
                                let (err_code, risk_event_type, reason) =
                                    if synthetic_price.is_some() {
                                        (
                                            "confirm_observed_fill_unavailable_manual_reconcile_required",
                                            "execution_confirm_observed_fill_unavailable_manual_reconcile_required",
                                            "unusable_observed_fill",
                                        )
                                    } else {
                                        (
                                            "confirm_price_unavailable_manual_reconcile_required",
                                            "execution_confirm_price_unavailable_manual_reconcile_required",
                                            "missing_latest_price_no_fallback",
                                        )
                                    };
                                return self.skip_confirm_manual_reconcile(
                                    store,
                                    intent,
                                    order_id,
                                    lifecycle_status,
                                    route,
                                    now,
                                    report,
                                    err_code,
                                    risk_event_type,
                                    json!({
                                        "signal_id": intent.signal_id,
                                        "order_id": order_id,
                                        "token": intent.token,
                                        "route": route,
                                        "err_code": err_code,
                                        "reason": reason,
                                        "manual_reconcile_required": true,
                                        "error": error.to_string(),
                                    }),
                                );
                            }
                            warn!(
                                signal_id = %intent.signal_id,
                                order_id,
                                token = %intent.token,
                                route,
                                error = %error,
                                "observed confirmed fill unusable; falling back to synthetic execution fill"
                            );
                            let Some(resolution) = synthetic_price else {
                                let result = fail_confirm_price_unavailable()?;
                                return Ok(result);
                            };
                            let (fill, used_price_fallback, fallback_source) =
                                build_synthetic_fill(resolution)?;
                            (Some(fill), used_price_fallback, fallback_source)
                        }
                    }
                } else {
                    let synthetic_price =
                        self.resolve_synthetic_price(store, intent, route, confirmed_at)?;
                    if live_manual_reconcile_fail_closed {
                        let (err_code, risk_event_type, reason) = if synthetic_price.is_some() {
                            (
                                "confirm_observed_fill_unavailable_manual_reconcile_required",
                                "execution_confirm_observed_fill_unavailable_manual_reconcile_required",
                                "missing_observed_fill",
                            )
                        } else {
                            (
                                "confirm_price_unavailable_manual_reconcile_required",
                                "execution_confirm_price_unavailable_manual_reconcile_required",
                                "missing_latest_price_no_fallback",
                            )
                        };
                        return self.skip_confirm_manual_reconcile(
                            store,
                            intent,
                            order_id,
                            lifecycle_status,
                            route,
                            now,
                            report,
                            err_code,
                            risk_event_type,
                            json!({
                                "signal_id": intent.signal_id,
                                "order_id": order_id,
                                "token": intent.token,
                                "route": route,
                                "err_code": err_code,
                                "reason": reason,
                                "manual_reconcile_required": true,
                            }),
                        );
                    }
                    if self.mode == "adapter_submit_confirm" {
                        warn!(
                            signal_id = %intent.signal_id,
                            order_id,
                            token = %intent.token,
                            route,
                            "confirmed transaction missing observed on-chain fill; falling back to synthetic execution fill"
                        );
                    }
                    let Some(resolution) = synthetic_price else {
                        let result = fail_confirm_price_unavailable()?;
                        return Ok(result);
                    };
                    let (fill, used_price_fallback, fallback_source) =
                        build_synthetic_fill(resolution)?;
                    (Some(fill), used_price_fallback, fallback_source)
                };
                let Some(fill) = fill else {
                    return Ok(SignalResult::Failed);
                };
                let mut post_confirm_risk_breach = None;
                match store.finalize_execution_confirmed_order(
                    &fill.order_id,
                    &intent.signal_id,
                    &fill.token,
                    intent.side.as_str(),
                    fill.qty,
                    fill.notional_sol,
                    fill.avg_price_sol,
                    fill.fee_sol,
                    fill.slippage_bps,
                    confirmed_at,
                )? {
                    FinalizeExecutionConfirmOutcome::Applied(snapshot) => {
                        post_confirm_risk_breach = self.confirmed_buy_risk_breach(intent, snapshot);
                    }
                    FinalizeExecutionConfirmOutcome::AlreadyConfirmed => {
                        store
                            .update_copy_signal_status(&intent.signal_id, "execution_confirmed")?;
                    }
                }
                if let Some(breach) = post_confirm_risk_breach {
                    let details = json!({
                        "signal_id": intent.signal_id,
                        "order_id": order_id,
                        "token": intent.token,
                        "route": route,
                        "reason": "post_confirm_risk_recheck_breach",
                        "breaches": breach.reasons,
                        "total_exposure_sol": breach.total_exposure_sol,
                        "max_total_exposure_sol": self.risk.max_total_exposure_sol,
                        "token_exposure_sol": breach.token_exposure_sol,
                        "max_exposure_per_token_sol": self.risk.max_exposure_per_token_sol,
                        "open_positions": breach.open_positions,
                        "max_concurrent_positions": self.risk.max_concurrent_positions,
                        "confirmed_at": confirmed_at.to_rfc3339(),
                    })
                    .to_string();
                    let _ = store.insert_risk_event(
                        "execution_confirm_risk_breach",
                        "error",
                        confirmed_at,
                        Some(&details),
                    );
                    warn!(
                        signal_id = %intent.signal_id,
                        order_id,
                        token = %intent.token,
                        route,
                        total_exposure_sol = breach.total_exposure_sol,
                        token_exposure_sol = breach.token_exposure_sol,
                        open_positions = breach.open_positions,
                        reasons = ?breach.reasons,
                        "confirmed BUY breached runtime risk limits after finalize"
                    );
                }
                if confirm.network_fee_lamports.is_none() && self.mode == "adapter_submit_confirm" {
                    let network_fee_lookup_error = confirm
                        .network_fee_lookup_error
                        .as_deref()
                        .unwrap_or_default();
                    let network_fee_source = if network_fee_lamports_hint.is_some() {
                        "adapter_hint"
                    } else {
                        "none"
                    };
                    let details = json!({
                        "signal_id": intent.signal_id,
                        "order_id": order_id,
                        "route": route,
                        "network_fee_lamports": if network_fee_lamports_hint.is_some() {
                            serde_json::Value::from(resolved_network_fee_lamports)
                        } else {
                            serde_json::Value::Null
                        },
                        "network_fee_lookup_error_class": if network_fee_lookup_error.is_empty() { serde_json::Value::Null } else { serde_json::Value::String(network_fee_lookup_error.to_string()) },
                        "network_fee_missing_reason": if network_fee_lookup_error.is_empty() { "meta_fee_unavailable" } else { "rpc_lookup_error" },
                        "network_fee_source": network_fee_source,
                        "base_fee_lamports_hint": base_fee_lamports_hint,
                        "priority_fee_lamports_hint": priority_fee_lamports_hint,
                        "tip_lamports": route_tip_lamports,
                        "ata_create_rent_lamports": ata_create_rent_lamports,
                        "fee_sol_applied": execution_fee_sol,
                        "reason": if network_fee_lamports_hint.is_some() {
                            "missing_network_fee_from_confirmation_using_submit_hint"
                        } else {
                            "missing_network_fee_from_confirmation"
                        },
                        "manual_reconcile_recommended": network_fee_lamports_hint.is_none()
                    })
                    .to_string();
                    let _ = store.insert_risk_event(
                        if network_fee_lamports_hint.is_some() {
                            "execution_network_fee_unavailable_submit_hint_used"
                        } else {
                            "execution_network_fee_unavailable_fallback_used"
                        },
                        if network_fee_lamports_hint.is_some() {
                            "warn"
                        } else {
                            "error"
                        },
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
                        "fallback_avg_price_sol": fill.avg_price_sol,
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
                if manual_reconcile_required {
                    if lifecycle_status != EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS {
                        store.mark_order_reconcile_pending(order_id, err_code)?;
                        store.update_copy_signal_status(
                            &intent.signal_id,
                            EXECUTION_SUBMITTED_RECONCILE_PENDING_STATUS,
                        )?;
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
                    bump_route_counter(&mut report.confirm_retry_scheduled_by_route, route);
                    return Ok(SignalResult::Skipped);
                }
                store.mark_order_failed(order_id, err_code, Some(confirm.detail.as_str()))?;
                store.update_copy_signal_status(&intent.signal_id, "execution_failed")?;
                bump_route_counter(&mut report.confirm_failed_by_route, route);
                Ok(SignalResult::Failed)
            }
        }
    }
}
