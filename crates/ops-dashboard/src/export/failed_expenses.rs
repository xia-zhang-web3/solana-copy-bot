use super::{
    io::InputReport,
    json::{row, row_text},
};

pub(super) fn rows(report: &InputReport) -> Vec<Vec<String>> {
    let fees = report
        .value
        .as_ref()
        .and_then(|v| v.get("tiny_execution_proof"))
        .and_then(|v| v.get("failed_expenses"));
    let count = |key: &str| fees.and_then(|v| v.get(key)).and_then(|v| v.as_u64());
    let text = |key: &str| {
        fees.and_then(|v| v.get(key))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
    };
    let mut rows = vec![
        row(
            "failed_expense_orders",
            count("total_orders"),
            "submitted failed orders in original operation window",
        ),
        row(
            "failed_expense_known_orders",
            count("known_orders"),
            "validated wallet fee subset",
        ),
        row(
            "failed_expense_unknown_orders",
            count("unknown_orders"),
            "wallet fee unknown",
        ),
        row(
            "failed_expense_unresolved_orders",
            count("unresolved_orders"),
            "unknown fee, conflict or unexplained native residual",
        ),
        row(
            "failed_expense_legacy_orders",
            count("legacy_uncovered_orders"),
            "historical failed rows without receipt accounting",
        ),
    ];
    for (key, source, detail) in [
        (
            "failed_expense_known_lamports",
            "known_wallet_fee_lamports",
            "known wallet network fee subset; expense recorded once",
        ),
        (
            "failed_expense_cohort_lamports",
            "cohort_wallet_fee_lamports",
            "unknown for incomplete or empty cohort",
        ),
        (
            "failed_expense_native_delta_lamports",
            "known_native_delta_lamports",
            "separate native observation; covered subset",
        ),
        (
            "failed_expense_unexplained_lamports",
            "known_unexplained_delta_lamports",
            "native delta plus wallet fee; covered subset",
        ),
        (
            "failed_expense_coverage",
            "coverage",
            "selected cohort only",
        ),
        (
            "failed_expense_basis",
            "source_basis",
            "failed receipt proof",
        ),
        (
            "failed_expense_window",
            "window_basis",
            "late receipt remains in original operation window",
        ),
        ("failed_expense_since", "since", "inclusive"),
        ("failed_expense_as_of", "as_of", "exclusive"),
        (
            "failed_expense_history",
            "history_coverage",
            "empty ledger does not prove no past costs",
        ),
    ] {
        rows.push(row_text(key, text(source), detail, "warning"));
    }
    rows
}
