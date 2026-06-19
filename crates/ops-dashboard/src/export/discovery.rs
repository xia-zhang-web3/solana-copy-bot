use serde_json::{json, Value};

use super::{
    io::InputReport,
    json::{array_len, bool_path, f64_path, row, row_text, u64_path, u64_path_ref},
};

pub(super) fn discovery_snapshot(
    report: &InputReport,
    wallet_report: &InputReport,
    candidate_floor: u64,
) -> (bool, Value) {
    let candidates = array_len(&report.value, &["candidate_wallets"]);
    let mut rows = vec![
        row("candidates", candidates, "published candidate wallets"),
        row("floor", Some(candidate_floor), "configured dashboard floor"),
        row(
            "eligible_wallets",
            u64_path(&report.value, &["wallet_metrics_total"]),
            "wallet metrics total",
        ),
        row(
            "scan_rows",
            u64_path(&report.value, &["scan", "rows_scanned"]),
            "discovery scan",
        ),
        row(
            "tail_lag_seconds",
            u64_path(&report.value, &["max_tail_lag_seconds"]),
            "observed swaps tail",
        ),
    ];
    rows.extend(discovery_filter_rows(report, wallet_report));
    let status = if report.status.stale {
        "unknown"
    } else if candidates.unwrap_or(0) < candidate_floor {
        "below_floor"
    } else {
        "healthy"
    };
    (
        report.status.stale || wallet_report.status.stale,
        json!({ "status": status, "rows": rows }),
    )
}

fn discovery_filter_rows(report: &InputReport, wallet_report: &InputReport) -> Vec<Vec<String>> {
    let filter = wallet_report
        .value
        .as_ref()
        .and_then(|value| value.get("filter_impact"));
    let rug_enabled = bool_path(filter, &["rug_filter_enabled"]);
    let mut rows = vec![row_text(
        "rug_filter",
        match rug_enabled {
            Some(true) => "on",
            Some(false) => "off",
            None => "not_reported",
        },
        if wallet_report.status.stale {
            "wallet report stale or missing"
        } else {
            "wallet report"
        },
        if rug_enabled == Some(true) {
            "safe"
        } else {
            "warning"
        },
    )];
    rows.push(row(
        "rug_min_closed",
        u64_path_ref(filter, &["rug_filter_min_closed_trades"]),
        "minimum closed trades",
    ));
    rows.push(row_text(
        "rug_rate",
        &f64_path(filter, &["rug_filter_max_stale_terminal_rate"])
            .map(|rate| format!("{:.0}%", rate * 100.0))
            .unwrap_or_else(|| "unknown".to_string()),
        "stale/terminal rate threshold",
        if filter.is_some() { "safe" } else { "warning" },
    ));
    rows.push(row_text(
        "rug_pnl_floor",
        &f64_path(filter, &["rug_filter_max_stale_terminal_pnl_sol"])
            .map(|pnl| format!("{pnl:.2} SOL"))
            .unwrap_or_else(|| "unknown".to_string()),
        "stale/terminal PnL floor",
        if filter.is_some() { "safe" } else { "warning" },
    ));
    rows.push(row(
        "rug_quarantine_hours",
        u64_path_ref(filter, &["rug_filter_quarantine_hours"]),
        "sticky quarantine window",
    ));
    rows.push(row(
        "rug_rejected_returned",
        u64_path_ref(filter, &["rug_rejected_returned"]),
        "rejected in returned metrics",
    ));
    rows.push(row(
        "rug_quarantine_candidates",
        array_len(&report.value, &["rug_quarantine_candidates"]),
        "new quarantine candidates",
    ));
    rows
}
