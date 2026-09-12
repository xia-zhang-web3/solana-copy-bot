use crate::{
    ExecutionCanaryOrder, ExecutionSourceSellWriteOffKind as Kind, EXECUTION_ERROR_BUILD_FAILED,
    EXECUTION_ERROR_SIMULATION_FAILED, EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE,
    EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED, EXECUTION_STATUS_CANARY_FAILED,
};

// Shared with the legacy runtime classifier: automatic terminal policy is unchanged.
pub fn execution_terminal_sell_no_route_proof(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("no_routes_found")
        || lower.contains("no routes found")
        || lower.contains("token_not_tradable")
        || lower.contains("not tradable")
        || lower.contains("bonding curve for mint not found")
}

pub(crate) fn refusal(order: &ExecutionCanaryOrder, kind: Kind) -> Option<&'static str> {
    if matches!(
        order.err_code.as_deref(),
        Some(
            EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE
                | EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED
        )
    ) {
        return Some("source_sell_write_off_already_terminal");
    }
    if order.status != EXECUTION_STATUS_CANARY_FAILED {
        return Some("source_sell_write_off_order_not_failed");
    }
    if order
        .tx_signature
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty())
    {
        return Some("source_sell_write_off_signature_present");
    }
    let expected = if matches!(kind, Kind::TerminalSimulation { .. }) {
        EXECUTION_ERROR_SIMULATION_FAILED
    } else {
        EXECUTION_ERROR_BUILD_FAILED
    };
    if order.err_code.as_deref() != Some(expected) {
        return Some("source_sell_write_off_error_mismatch");
    }
    match kind {
        Kind::TerminalSimulation { max_attempts } | Kind::TerminalNoRoute { max_attempts }
            if order.attempt < max_attempts.max(1) =>
        {
            return Some("source_sell_write_off_attempts_remaining")
        }
        _ => {}
    }
    let error = order.simulation_error.as_deref().unwrap_or_default();
    match kind {
        Kind::TerminalNoRoute { .. } if !execution_terminal_sell_no_route_proof(error) => {
            Some("source_sell_write_off_no_route_unproven")
        }
        Kind::DustNoRoute if !error.contains("NO_ROUTES_FOUND") => {
            Some("source_sell_write_off_dust_error_mismatch")
        }
        _ => None,
    }
}
