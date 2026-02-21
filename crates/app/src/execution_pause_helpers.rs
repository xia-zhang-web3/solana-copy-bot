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
