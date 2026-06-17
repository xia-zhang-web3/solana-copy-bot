use serde_json::{json, Value};

use super::{
    io::InputReport,
    json::{
        array_len, bool_path, display_u64, f64_path, row, row_bytes, row_float, storage_level,
        str_path, u64_path, u64_path_ref,
    },
    ExportOptions,
};

pub(super) fn overview_snapshot(
    execution: &InputReport,
    discovery: &InputReport,
    wal: &InputReport,
    capacity: &InputReport,
    options: &ExportOptions,
) -> (bool, Value) {
    let candidates = array_len(&discovery.value, &["candidate_wallets"]);
    let blockers = array_len(&discovery.value, &["blockers"]);
    let open_positions = u64_path(
        &execution.value,
        &["tiny_execution_quality", "tiny_open_positions"],
    )
    .or_else(|| {
        u64_path(
            &execution.value,
            &["tiny_execution_proof", "summary", "tiny_open_positions"],
        )
    });
    let unmatched = u64_path(
        &execution.value,
        &["wallet_reconciliation", "unmatched_open_position_count"],
    );
    let gate = execution
        .value
        .as_ref()
        .and_then(|value| value.get("tiny_execution_gate"));
    let entries = bool_path(gate, &["can_open_new_tiny_entries"])
        .map(|enabled| if enabled { "active" } else { "paused" })
        .unwrap_or("unknown");
    let sells = bool_path(gate, &["can_process_tiny_sells"])
        .map(|enabled| if enabled { "active" } else { "paused" })
        .unwrap_or("unknown");
    let wal_level = str_path(&wal.value, &["wal_pressure_level"]).unwrap_or("unknown");
    let stale = execution.status.stale
        || discovery.status.stale
        || wal.status.stale
        || capacity.status.stale;
    let disk_runway_days = disk_runway_days(capacity);
    let status = overview_status(stale, candidates, unmatched, blockers, wal_level, options);

    (
        stale,
        json!({
            "status": status,
            "entries": entries,
            "sells": sells,
            "open_positions": display_u64(open_positions),
            "disk_runway_days": disk_runway_days,
            "disk_free_bytes": u64_path(&capacity.value, &["available_bytes"]),
            "candidates": display_u64(candidates),
            "candidate_floor": options.candidate_floor,
            "latest_blocker": latest_blocker(execution, discovery, wal, capacity, candidates, unmatched, blockers)
        }),
    )
}

pub(super) fn execution_snapshot(report: &InputReport) -> (bool, Value) {
    let quality = report
        .value
        .as_ref()
        .and_then(|value| value.get("tiny_execution_quality"));
    let summary = report
        .value
        .as_ref()
        .and_then(|value| value.get("tiny_execution_proof"))
        .and_then(|value| value.get("summary"));
    let wallet = report
        .value
        .as_ref()
        .and_then(|value| value.get("wallet_reconciliation"));
    let rows = vec![
        row(
            "entry_confirmed",
            u64_path_ref(quality, &["tiny_entry_confirmed_trades"])
                .or_else(|| u64_path_ref(summary, &["tiny_entry_confirmed_trades"])),
            "confirmed tiny entries",
        ),
        row(
            "exit_confirmed",
            u64_path_ref(quality, &["tiny_exit_confirmed_trades"])
                .or_else(|| u64_path_ref(summary, &["tiny_exit_confirmed_trades"])),
            "confirmed tiny exits",
        ),
        row(
            "open_positions",
            u64_path_ref(quality, &["tiny_open_positions"])
                .or_else(|| u64_path_ref(summary, &["tiny_open_positions"])),
            "tiny book",
        ),
        row(
            "wallet_unmatched",
            u64_path_ref(wallet, &["unmatched_open_position_count"]),
            "wallet reconciliation",
        ),
    ];
    let status = str_path(&report.value, &["reason_class"]).unwrap_or("unknown");
    (
        report.status.stale,
        json!({ "status": status, "rows": rows }),
    )
}

pub(super) fn discovery_snapshot(report: &InputReport, candidate_floor: u64) -> (bool, Value) {
    let candidates = array_len(&report.value, &["candidate_wallets"]);
    let rows = vec![
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
    let status = if report.status.stale {
        "unknown"
    } else if candidates.unwrap_or(0) < candidate_floor {
        "below_floor"
    } else {
        "healthy"
    };
    (
        report.status.stale,
        json!({ "status": status, "rows": rows }),
    )
}

pub(super) fn storage_snapshot(report: &InputReport, capacity: &InputReport) -> (bool, Value) {
    let level = str_path(&report.value, &["wal_pressure_level"]).unwrap_or("unknown");
    let rows = vec![
        row_bytes(
            "volume_free",
            u64_path(&capacity.value, &["available_bytes"]),
            "state volume free",
        ),
        row(
            "disk_runway_days",
            disk_runway_days(capacity),
            "free-space runway",
        ),
        row_bytes(
            "database",
            u64_path(&report.value, &["db_bytes"]),
            "primary data store",
        ),
        row_bytes("wal", u64_path(&report.value, &["wal_bytes"]), "SQLite WAL"),
        row_bytes("shm", u64_path(&report.value, &["shm_bytes"]), "SQLite SHM"),
        vec![
            "wal_pressure".to_string(),
            level.to_string(),
            str_path(&report.value, &["wal_pressure_reason"])
                .unwrap_or("snapshot")
                .to_string(),
            storage_level(level).to_string(),
        ],
    ];
    (
        report.status.stale || capacity.status.stale,
        json!({ "status": level, "rows": rows }),
    )
}

pub(super) fn strategy_snapshot(strategy: &InputReport, execution: &InputReport) -> (bool, Value) {
    if let Some(value) = &strategy.value {
        return (
            strategy.status.stale,
            value.get("data").cloned().unwrap_or_else(|| value.clone()),
        );
    }
    let summary = execution
        .value
        .as_ref()
        .and_then(|value| value.get("summary"));
    let rows = vec![
        row_float(
            "shadow_pnl",
            f64_path(summary, &["shadow_pnl_sol"]),
            "limit-matched source report",
        ),
        row_float(
            "quote_after_priority",
            f64_path(summary, &["quote_adjusted_pnl_after_priority_fee_sol"]),
            "executable estimate",
        ),
        row_float(
            "quote_vs_shadow_delta",
            f64_path(summary, &["quote_after_fee_vs_shadow_delta_sol"]),
            "tail drag",
        ),
        vec![
            "green_criterion".to_string(),
            "not_ready".to_string(),
            "multi-window validation required".to_string(),
            "warning".to_string(),
        ],
    ];
    (
        execution.status.stale,
        json!({ "status": "not_green", "rows": rows }),
    )
}

pub(super) fn alerts_snapshot(
    execution: &InputReport,
    discovery: &InputReport,
    wal: &InputReport,
    capacity: &InputReport,
    options: &ExportOptions,
) -> (bool, Value) {
    let mut events = Vec::new();
    if discovery.status.stale {
        events.push(alert(
            "Discovery snapshot stale",
            "candidate floor cannot be trusted",
            "warning",
        ));
    }
    if execution.status.stale {
        events.push(alert(
            "Execution snapshot stale",
            "tiny execution state unknown",
            "warning",
        ));
    }
    if wal.status.stale {
        events.push(alert(
            "Storage snapshot stale",
            "disk/WAL state unknown",
            "warning",
        ));
    }
    if capacity.status.stale {
        events.push(alert(
            "Storage capacity snapshot stale",
            "free-space runway unknown",
            "warning",
        ));
    }
    if array_len(&discovery.value, &["candidate_wallets"]).unwrap_or(0) < options.candidate_floor {
        events.push(alert(
            "Candidates below floor",
            "publication may fail closed",
            "dangerous",
        ));
    }
    if str_path(&wal.value, &["wal_pressure_level"]) == Some("critical") {
        events.push(alert(
            "WAL pressure critical",
            "maintenance action required",
            "dangerous",
        ));
    }
    if events.is_empty() {
        events.push(json!({
            "title": "No critical transitions",
            "detail": "Snapshot exporter found no active alert candidates.",
            "state": "resolved",
            "level": "safe"
        }));
    }
    let stale = execution.status.stale
        || discovery.status.stale
        || wal.status.stale
        || capacity.status.stale;
    (stale, json!({ "status": "available", "events": events }))
}

pub(super) fn reports_snapshot(inputs: &[&InputReport]) -> (bool, Value) {
    let rows: Vec<_> = inputs
        .iter()
        .map(|input| {
            vec![
                input.status.name.clone(),
                if input.status.loaded {
                    "loaded"
                } else {
                    "missing"
                }
                .to_string(),
                input
                    .status
                    .age_ms
                    .map(|age| format!("{age}ms"))
                    .unwrap_or_else(|| input.status.error.clone().unwrap_or_default()),
                if input.status.stale {
                    "warning"
                } else {
                    "safe"
                }
                .to_string(),
            ]
        })
        .collect();
    let stale = inputs.iter().any(|input| input.status.stale);
    (stale, json!({ "status": "available", "rows": rows }))
}

fn overview_status(
    stale: bool,
    candidates: Option<u64>,
    unmatched: Option<u64>,
    blockers: Option<u64>,
    wal_level: &str,
    options: &ExportOptions,
) -> &'static str {
    if stale {
        return "unknown";
    }
    if candidates.unwrap_or(0) < options.candidate_floor
        || unmatched.unwrap_or(0) > 0
        || blockers.unwrap_or(0) > 0
        || wal_level == "critical"
    {
        "critical"
    } else {
        "green"
    }
}

fn latest_blocker(
    execution: &InputReport,
    discovery: &InputReport,
    wal: &InputReport,
    capacity: &InputReport,
    candidates: Option<u64>,
    unmatched: Option<u64>,
    blockers: Option<u64>,
) -> &'static str {
    if execution.status.stale {
        "execution_snapshot_stale"
    } else if discovery.status.stale {
        "discovery_snapshot_stale"
    } else if wal.status.stale {
        "storage_snapshot_stale"
    } else if capacity.status.stale {
        "storage_capacity_snapshot_stale"
    } else if candidates.unwrap_or(0) == 0 {
        "candidate_wallets_empty"
    } else if unmatched.unwrap_or(0) > 0 {
        "wallet_reconciliation_unmatched"
    } else if blockers.unwrap_or(0) > 0 {
        "discovery_blockers_present"
    } else {
        "none_active"
    }
}

fn disk_runway_days(capacity: &InputReport) -> Option<u64> {
    u64_path(&capacity.value, &["runway_days"])
}

fn alert(title: &str, detail: &str, level: &str) -> Value {
    json!({ "title": title, "detail": detail, "state": "active", "level": level })
}
