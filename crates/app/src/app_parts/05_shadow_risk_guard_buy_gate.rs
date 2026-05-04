impl ShadowRiskGuard {
    fn can_open_buy(
        &mut self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        pause_new_trades_on_outage: bool,
    ) -> BuyRiskDecision {
        if !self.config.shadow_killswitch_enabled {
            return BuyRiskDecision::Allow;
        }

        if let Err(error) = self.maybe_refresh_db_state(store, now) {
            let fail_closed_detail = format!("risk_check_error: {error}");
            let should_log = self.on_risk_refresh_error(now);
            if should_log {
                warn!(error = %error, "shadow risk fail-closed activated");
                let details_json = format!(
                    "{{\"error\":\"{}\"}}",
                    sanitize_json_value(&error.to_string())
                );
                if let Err(event_error) =
                    persist_shadow_risk_fail_closed_event_or_warn(store, now, &details_json)
                {
                    return BuyRiskDecision::Blocked {
                        reason: BuyRiskBlockReason::FailClosed,
                        detail: format!(
                            "{fail_closed_detail}; fail_closed_event_error: {event_error:#}"
                        ),
                    };
                }
            }
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail: fail_closed_detail,
            };
        }

        if let Some(reason) = self.hard_stop_reason.as_deref() {
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::HardStop,
                detail: reason.to_string(),
            };
        }

        if self.exposure_hard_blocked {
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::ExposureCap,
                detail: self
                    .exposure_hard_detail
                    .clone()
                    .unwrap_or_else(|| "exposure_hard_cap_active".to_string()),
            };
        }

        if let Some(until) = self.pause_until {
            if now < until {
                return BuyRiskDecision::Blocked {
                    reason: BuyRiskBlockReason::TimedPause,
                    detail: self
                        .pause_reason
                        .clone()
                        .unwrap_or_else(|| format!("paused_until={}", until.to_rfc3339())),
                };
            }
            if let Err(error) = self.clear_pause(store, now) {
                let fail_closed_detail = format!("timed_pause_clear_error: {error}");
                let should_log = self.on_risk_refresh_error(now);
                if should_log {
                    warn!(error = %error, "shadow risk fail-closed activated during timed pause clear");
                    let details_json = format!(
                        "{{\"error\":\"{}\"}}",
                        sanitize_json_value(&error.to_string())
                    );
                    if let Err(event_error) =
                        persist_shadow_risk_fail_closed_event_or_warn(store, now, &details_json)
                    {
                        return BuyRiskDecision::Blocked {
                            reason: BuyRiskBlockReason::FailClosed,
                            detail: format!(
                                "{fail_closed_detail}; fail_closed_event_error: {event_error:#}"
                            ),
                        };
                    }
                }
                return BuyRiskDecision::Blocked {
                    reason: BuyRiskBlockReason::FailClosed,
                    detail: fail_closed_detail,
                };
            }
        }

        if self.soft_exposure_pause_latched {
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail: self.soft_exposure_pause_reason.clone().unwrap_or_else(|| {
                    self.soft_exposure_pause_until
                        .map(|until| {
                            format!(
                                "exposure_soft_cap: latched_until_recovery_below_resume_threshold; initial_until={}",
                                until.to_rfc3339()
                            )
                        })
                        .unwrap_or_else(|| {
                            "exposure_soft_cap: latched_until_recovery_below_resume_threshold"
                                .to_string()
                        })
                }),
            };
        }

        if self.universe_blocked {
            return BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                detail: format!("universe_breach_streak={}", self.universe_breach_streak),
            };
        }

        if pause_new_trades_on_outage {
            if let Some(reason) = self.infra_block_reason.as_deref() {
                return BuyRiskDecision::Blocked {
                    reason: BuyRiskBlockReason::Infra,
                    detail: reason.to_string(),
                };
            }
        }

        BuyRiskDecision::Allow
    }

    fn on_risk_refresh_error(&mut self, now: DateTime<Utc>) -> bool {
        self.hard_stop_clear_healthy_streak = 0;
        let should_log = self
            .last_fail_closed_log_at
            .map(|logged_at| {
                now - logged_at
                    >= chrono::Duration::seconds(RISK_FAIL_CLOSED_LOG_THROTTLE_SECONDS.max(1))
            })
            .unwrap_or(true);
        if should_log {
            self.last_fail_closed_log_at = Some(now);
        }
        should_log
    }
}
