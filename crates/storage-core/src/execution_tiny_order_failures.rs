use crate::{
    ExecutionTinyOrderFailureCount, ExecutionTinyProofOrder, EXECUTION_STATUS_CANARY_CONFIRMED,
};
use std::collections::BTreeMap;

pub(crate) fn order_failure_counts(
    orders: &[ExecutionTinyProofOrder],
) -> Vec<ExecutionTinyOrderFailureCount> {
    let mut counts =
        BTreeMap::<(String, String, String, String, String, String, String), u64>::new();
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
                (side, status, err_code, simulation_status, decision_reason, route, quote_source),
                orders,
            )| ExecutionTinyOrderFailureCount {
                side,
                status,
                err_code,
                simulation_status,
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
