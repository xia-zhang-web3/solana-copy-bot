use super::{
    io::InputReport,
    json::{row, row_text},
};

pub(super) fn rows(report: &InputReport) -> Vec<Vec<String>> {
    let cash = report
        .value
        .as_ref()
        .and_then(|v| v.get("tiny_execution_proof"))
        .and_then(|v| v.get("cash_settlements"));
    let count = |key: &str| cash.and_then(|v| v.get(key)).and_then(|v| v.as_u64());
    let amount = |key: &str| {
        cash.and_then(|v| v.get(key))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
    };
    vec![
        row(
            "cash_settled_orders",
            count("settled_orders"),
            "receipt-native SELL, including owned-only",
        ),
        row(
            "cash_unsettled_orders",
            count("unsettled_orders"),
            "pending/unsupported SELL cohort",
        ),
        row_text(
            "known_cash_result_lamports",
            amount("known_cash_result_delta_lamports"),
            "known receipt-native subset minus allocated entry basis; not economic PnL",
            "warning",
        ),
        row_text(
            "cohort_cash_result_lamports",
            amount("cohort_cash_result_delta_lamports"),
            "unknown if legacy or unsettled orders are present",
            "warning",
        ),
        row_text(
            "economic_pnl",
            "unknown",
            "fees/rent/WSOL decomposition unresolved",
            "warning",
        ),
    ]
}
