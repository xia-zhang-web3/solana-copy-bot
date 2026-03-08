use tracing::{info, warn};

use super::{OperatorEmergencyStop, ShadowRiskGuard};

pub(crate) fn resolve_buy_submit_pause_reason(
    operator_emergency_stop: &OperatorEmergencyStop,
    shadow_risk_guard: &ShadowRiskGuard,
    pause_new_trades_on_outage: bool,
    execution_emergency_stop_active_logged: &mut bool,
    execution_hard_stop_pause_logged: &mut bool,
    execution_outage_pause_logged: &mut bool,
) -> Option<String> {
    let mut buy_submit_pause_reason: Option<String> = None;
    if operator_emergency_stop.is_active() {
        if !*execution_emergency_stop_active_logged {
            warn!(
                detail = %operator_emergency_stop.detail(),
                "execution BUY submission paused by operator emergency stop"
            );
            *execution_emergency_stop_active_logged = true;
        }
        buy_submit_pause_reason = Some(format!(
            "operator_emergency_stop: {}",
            operator_emergency_stop.detail()
        ));
    } else if *execution_emergency_stop_active_logged {
        info!("execution BUY submission resumed after operator emergency stop clear");
        *execution_emergency_stop_active_logged = false;
    }

    if let Some(reason) = shadow_risk_guard.hard_stop_reason.as_deref() {
        if !*execution_hard_stop_pause_logged {
            warn!(
                reason = %reason,
                "execution BUY submission paused by risk hard stop"
            );
            *execution_hard_stop_pause_logged = true;
        }
        if buy_submit_pause_reason.is_none() {
            buy_submit_pause_reason = Some(format!("risk_hard_stop: {}", reason));
        }
    } else if *execution_hard_stop_pause_logged {
        info!("execution BUY submission resumed after risk hard stop clear");
        *execution_hard_stop_pause_logged = false;
    }

    if shadow_risk_guard.exposure_hard_blocked && buy_submit_pause_reason.is_none() {
        buy_submit_pause_reason = Some(format!(
            "risk_exposure_hard_cap: {}",
            shadow_risk_guard
                .exposure_hard_detail
                .clone()
                .unwrap_or_else(|| "exposure_hard_cap_active".to_string())
        ));
    }

    if let Some(until) = shadow_risk_guard.pause_until {
        if chrono::Utc::now() < until && buy_submit_pause_reason.is_none() {
            buy_submit_pause_reason = Some(format!(
                "risk_timed_pause: {}",
                shadow_risk_guard
                    .pause_reason
                    .clone()
                    .unwrap_or_else(|| format!("paused_until={}", until.to_rfc3339()))
            ));
        }
    }

    if shadow_risk_guard.universe_blocked && buy_submit_pause_reason.is_none() {
        buy_submit_pause_reason = Some(format!(
            "risk_universe_stop: universe_breach_streak={}",
            shadow_risk_guard.universe_breach_streak
        ));
    }

    if let Some(error) = shadow_risk_guard.last_db_refresh_error.as_deref() {
        if buy_submit_pause_reason.is_none() {
            buy_submit_pause_reason = Some(format!("risk_fail_closed: {}", error));
        }
    }

    if pause_new_trades_on_outage {
        if let Some(reason) = shadow_risk_guard.infra_block_reason.as_deref() {
            if !*execution_outage_pause_logged {
                warn!(
                    reason = %reason,
                    "execution BUY submission paused due to infra outage gate"
                );
                *execution_outage_pause_logged = true;
            }
            if buy_submit_pause_reason.is_none() {
                buy_submit_pause_reason = Some(format!("risk_infra_stop: {}", reason));
            }
        } else if *execution_outage_pause_logged {
            info!("execution BUY submission resumed after infra outage gate cleared");
            *execution_outage_pause_logged = false;
        }
    } else if *execution_outage_pause_logged {
        info!("execution BUY submission resumed after infra outage gate disabled");
        *execution_outage_pause_logged = false;
    }

    buy_submit_pause_reason
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{Duration as StdDuration, Instant as StdInstant};

    use chrono::{Duration, Utc};

    use super::*;
    use crate::RiskConfig;

    fn inactive_operator_emergency_stop() -> OperatorEmergencyStop {
        OperatorEmergencyStop {
            path: PathBuf::from("/tmp/operator-stop"),
            poll_interval: StdDuration::from_millis(1000),
            next_refresh_at: StdInstant::now(),
            active: false,
            detail: String::new(),
        }
    }

    #[test]
    fn resolve_buy_submit_pause_reason_covers_shadow_risk_states() {
        let operator_emergency_stop = inactive_operator_emergency_stop();
        let mut guard = ShadowRiskGuard::new(RiskConfig::default());
        let mut emergency_logged = false;
        let mut hard_stop_logged = false;
        let mut outage_logged = false;

        guard.exposure_hard_blocked = true;
        guard.exposure_hard_detail = Some("hard_cap".to_string());
        assert_eq!(
            resolve_buy_submit_pause_reason(
                &operator_emergency_stop,
                &guard,
                true,
                &mut emergency_logged,
                &mut hard_stop_logged,
                &mut outage_logged,
            )
            .as_deref(),
            Some("risk_exposure_hard_cap: hard_cap")
        );

        guard.exposure_hard_blocked = false;
        guard.exposure_hard_detail = None;
        guard.pause_until = Some(Utc::now() + Duration::minutes(5));
        guard.pause_reason = Some("drawdown_1h".to_string());
        assert_eq!(
            resolve_buy_submit_pause_reason(
                &operator_emergency_stop,
                &guard,
                true,
                &mut emergency_logged,
                &mut hard_stop_logged,
                &mut outage_logged,
            )
            .as_deref(),
            Some("risk_timed_pause: drawdown_1h")
        );

        guard.pause_until = None;
        guard.pause_reason = None;
        guard.universe_blocked = true;
        guard.universe_breach_streak = 3;
        assert_eq!(
            resolve_buy_submit_pause_reason(
                &operator_emergency_stop,
                &guard,
                true,
                &mut emergency_logged,
                &mut hard_stop_logged,
                &mut outage_logged,
            )
            .as_deref(),
            Some("risk_universe_stop: universe_breach_streak=3")
        );

        guard.universe_blocked = false;
        guard.last_db_refresh_error = Some("refresh failed".to_string());
        assert_eq!(
            resolve_buy_submit_pause_reason(
                &operator_emergency_stop,
                &guard,
                true,
                &mut emergency_logged,
                &mut hard_stop_logged,
                &mut outage_logged,
            )
            .as_deref(),
            Some("risk_fail_closed: refresh failed")
        );
    }

    #[test]
    fn resolve_buy_submit_pause_reason_preserves_existing_priority_order() {
        let mut operator_emergency_stop = inactive_operator_emergency_stop();
        operator_emergency_stop.active = true;
        operator_emergency_stop.detail = "operator".to_string();

        let mut guard = ShadowRiskGuard::new(RiskConfig::default());
        guard.hard_stop_reason = Some("hard_stop".to_string());
        guard.exposure_hard_blocked = true;
        guard.exposure_hard_detail = Some("hard_cap".to_string());
        guard.pause_until = Some(Utc::now() + Duration::minutes(5));
        guard.pause_reason = Some("drawdown_1h".to_string());
        guard.universe_blocked = true;
        guard.universe_breach_streak = 2;
        guard.last_db_refresh_error = Some("refresh failed".to_string());
        guard.infra_block_reason = Some("infra".to_string());

        let mut emergency_logged = false;
        let mut hard_stop_logged = false;
        let mut outage_logged = false;

        assert_eq!(
            resolve_buy_submit_pause_reason(
                &operator_emergency_stop,
                &guard,
                true,
                &mut emergency_logged,
                &mut hard_stop_logged,
                &mut outage_logged,
            )
            .as_deref(),
            Some("operator_emergency_stop: operator")
        );
    }
}
