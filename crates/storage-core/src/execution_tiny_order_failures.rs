use crate::{
    ExecutionTinyOrderFailureCount, ExecutionTinyProofOrder, EXECUTION_STATUS_CANARY_CONFIRMED,
};
use std::collections::BTreeMap;

pub(crate) fn order_failure_counts(
    orders: &[ExecutionTinyProofOrder],
) -> Vec<ExecutionTinyOrderFailureCount> {
    let mut counts = BTreeMap::<
        (
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        ),
        u64,
    >::new();
    for order in orders {
        if order.status == EXECUTION_STATUS_CANARY_CONFIRMED {
            continue;
        }
        *counts
            .entry((
                value_or_missing(order.side.as_deref()),
                order.status.clone(),
                value_or_missing(order.err_code.as_deref()),
                value_or_missing(order.simulation_status.as_deref()),
                simulation_error_class(order.simulation_error.as_deref()),
                value_or_missing(order.decision_reason.as_deref()),
                order.route.clone(),
                value_or_missing(order.quote_source.as_deref()),
            ))
            .or_insert(0) += 1;
    }
    counts
        .into_iter()
        .map(
            |(
                (
                    side,
                    status,
                    err_code,
                    simulation_status,
                    simulation_error_class,
                    decision_reason,
                    route,
                    quote_source,
                ),
                orders,
            )| ExecutionTinyOrderFailureCount {
                side,
                status,
                err_code,
                simulation_status,
                simulation_error_class,
                decision_reason,
                route,
                quote_source,
                orders,
            },
        )
        .collect()
}

fn value_or_missing(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("missing")
        .to_string()
}

fn simulation_error_class(value: Option<&str>) -> String {
    let Some(raw) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return "missing".to_string();
    };
    let lower = raw.to_ascii_lowercase();
    if let Some(hex) = custom_program_error_code(&lower) {
        return format!("custom_program_error:{hex}");
    }
    if lower.contains("insufficient funds") {
        return "insufficient_funds".to_string();
    }
    if lower.contains("account not found") || lower.contains("could not find account") {
        return "account_not_found".to_string();
    }
    if lower.contains("blockhash") {
        return "blockhash".to_string();
    }
    if lower.contains("slippage") {
        return "slippage".to_string();
    }
    "other".to_string()
}

fn custom_program_error_code(lower: &str) -> Option<String> {
    let marker = "custom program error:";
    let (_, tail) = lower.split_once(marker)?;
    let code = tail
        .trim_start()
        .split(|ch: char| ch.is_whitespace() || ch == ',' || ch == ';' || ch == '}')
        .next()?
        .trim_matches(|ch: char| ch == '"' || ch == '\'' || ch == '.' || ch == ':');
    code.strip_prefix("0x")
        .filter(|hex| !hex.is_empty() && hex.chars().all(|ch| ch.is_ascii_hexdigit()))
        .map(|hex| format!("0x{hex}"))
}
