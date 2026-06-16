use anyhow::Result;
use chrono::{Duration as ChronoDuration, Utc};
use std::env;
use std::path::Path;

use super::cli::{
    load_service_state_from_systemctl, parse_args_from, resolve_runtime_db_path, validate_cli, Cli,
    RuntimeSqliteRetentionReport, ServiceState, USAGE,
};
use super::common::{compact_error, inspect_runtime_db_files, RuntimeDbFiles};
use super::ops::{execute_commit, ExecutedMaintenance};

const OUTCOME_COMPLETED: &str = "completed";
const OUTCOME_DRY_RUN: &str = "dry_run";
const OUTCOME_FAILED_SERVICE_ACTIVE: &str = "failed_service_active";
const OUTCOME_FAILED_UNPROVEN: &str = "failed_unproven";
const OUTCOME_SKIPPED_NOOP: &str = "skipped_noop";

const REASON_COMPLETED: &str = "runtime_sqlite_retention_maintenance_completed";
const REASON_DRY_RUN: &str = "runtime_sqlite_retention_maintenance_dry_run";
const REASON_NOOP: &str = "runtime_sqlite_retention_maintenance_noop";
const REASON_SERVICE_ACTIVE: &str = "runtime_sqlite_retention_service_active";
const REASON_SERVICE_NOT_INACTIVE: &str = "runtime_sqlite_retention_service_not_inactive";
const ACTION_COMPLETED: &str = "verify DB size, WAL size, and restart service if it was stopped";
const ACTION_DRY_RUN: &str = "rerun with --commit during maintenance window to execute";
const ACTION_NOOP: &str = "no retention action requested";
const ACTION_SERVICE_ACTIVE: &str =
    "stop service before runtime SQLite retention maintenance, then rerun";

pub fn main_entry() {
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(Some(cli)) => run_with_hooks(&cli, load_service_state_from_systemctl),
        Ok(None) => {
            println!("{USAGE}");
            return;
        }
        Err(error) => failed_report(
            &Cli::default_for_error(),
            None,
            None,
            None,
            Some(compact_error(error)),
        ),
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("retention report must serialize")
    );
    std::process::exit(report.exit_code());
}

pub(super) fn run_with_hooks<ServiceProbe>(
    cli: &Cli,
    service_probe: ServiceProbe,
) -> RuntimeSqliteRetentionReport
where
    ServiceProbe: Fn(&str) -> Result<ServiceState>,
{
    if let Err(error) = validate_cli(cli) {
        return failed_report(cli, None, None, None, Some(compact_error(error)));
    }
    let (db_path, observed_days) = match resolve_runtime_db_path(cli) {
        Ok(resolved) => resolved,
        Err(error) => return failed_report(cli, None, None, None, Some(compact_error(error))),
    };
    let before = match inspect_runtime_db_files(&db_path) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return failed_report(cli, Some(&db_path), None, None, Some(compact_error(error)))
        }
    };
    let service = match service_probe(&cli.service_name) {
        Ok(service) => service,
        Err(error) => {
            return failed_report(
                cli,
                Some(&db_path),
                Some(&before),
                None,
                Some(compact_error(error)),
            )
        }
    };
    if service.active_state != "inactive" && !cli.allow_service_active {
        let reason = if service.active_state == "active" {
            REASON_SERVICE_ACTIVE
        } else {
            REASON_SERVICE_NOT_INACTIVE
        };
        return base_report(
            cli,
            Some(&db_path),
            Some(&before),
            None,
            Some(&service),
            OUTCOME_FAILED_SERVICE_ACTIVE,
            reason,
            ACTION_SERVICE_ACTIVE,
            Some("service must be inactive for retention maintenance".to_string()),
        );
    }

    let observed_cutoff = Utc::now() - ChronoDuration::days(observed_days as i64);
    let canary_cutoff = Utc::now() - ChronoDuration::days(cli.canary_retention_days as i64);
    if !action_requested(cli) {
        return base_report(
            cli,
            Some(&db_path),
            Some(&before),
            None,
            Some(&service),
            OUTCOME_SKIPPED_NOOP,
            REASON_NOOP,
            ACTION_NOOP,
            None,
        )
        .with_cutoffs(observed_cutoff, canary_cutoff);
    }
    if !cli.commit {
        return base_report(
            cli,
            Some(&db_path),
            Some(&before),
            Some(&before),
            Some(&service),
            OUTCOME_DRY_RUN,
            REASON_DRY_RUN,
            ACTION_DRY_RUN,
            None,
        )
        .with_cutoffs(observed_cutoff, canary_cutoff);
    }

    match execute_commit(cli, &db_path, observed_cutoff, canary_cutoff) {
        Ok(executed) => {
            let after = inspect_runtime_db_files(&db_path).ok();
            completed_report(cli, &db_path, &before, after.as_ref(), &service, executed)
        }
        Err(error) => failed_report(
            cli,
            Some(&db_path),
            Some(&before),
            Some(&service),
            Some(compact_error(error)),
        ),
    }
}

fn completed_report(
    cli: &Cli,
    db_path: &Path,
    before: &RuntimeDbFiles,
    after: Option<&RuntimeDbFiles>,
    service: &ServiceState,
    executed: ExecutedMaintenance,
) -> RuntimeSqliteRetentionReport {
    let mut report = base_report(
        cli,
        Some(db_path),
        Some(before),
        after,
        Some(service),
        OUTCOME_COMPLETED,
        REASON_COMPLETED,
        ACTION_COMPLETED,
        None,
    )
    .with_cutoffs(executed.observed_cutoff, executed.canary_cutoff);
    report.production_green = true;
    report.observed_deleted_rows = executed.observed.rows;
    report.observed_delete_batches = executed.observed.batches;
    report.canary_event_deleted_rows = executed.canary_events.rows;
    report.canary_provider_sample_deleted_rows = executed.canary_provider_samples.rows;
    report.canary_shadow_gate_deleted_rows = executed.canary_shadow_gate.rows;
    report.canary_delete_batches = executed.canary_events.batches
        + executed.canary_provider_samples.batches
        + executed.canary_shadow_gate.batches;
    report.canary_ts_indexes_created = executed.canary_ts_indexes_created;
    report.checkpoint_truncate_attempted = executed.checkpoint.is_some();
    if let Some(checkpoint) = executed.checkpoint {
        report.checkpoint_busy = Some(checkpoint.busy);
        report.checkpoint_log_frames = Some(checkpoint.log_frames);
        report.checkpoint_checkpointed_frames = Some(checkpoint.checkpointed_frames);
    }
    report.vacuum_attempted = executed.vacuum_attempted;
    report
}

fn action_requested(cli: &Cli) -> bool {
    cli.max_observed_rows > 0
        || cli.max_canary_rows > 0
        || cli.create_canary_ts_indexes
        || cli.checkpoint_truncate
        || cli.vacuum_into.is_some()
}

fn base_report(
    cli: &Cli,
    db_path: Option<&Path>,
    before: Option<&RuntimeDbFiles>,
    after: Option<&RuntimeDbFiles>,
    service: Option<&ServiceState>,
    outcome: &str,
    reason: &str,
    action: &str,
    error: Option<String>,
) -> RuntimeSqliteRetentionReport {
    RuntimeSqliteRetentionReport {
        production_green: false,
        committed: cli.commit,
        service_name: cli.service_name.clone(),
        service_active_state: service.map(|service| service.active_state.clone()),
        service_active: service.map(|service| service.active),
        service_substate: service.and_then(|service| service.substate.clone()),
        runtime_db_path: db_path.map(|path| path.display().to_string()),
        before_db_bytes: before.map(|snapshot| snapshot.db.bytes),
        before_wal_bytes: before.map(|snapshot| snapshot.wal.bytes),
        before_shm_bytes: before.map(|snapshot| snapshot.shm.bytes),
        after_db_bytes: after.map(|snapshot| snapshot.db.bytes),
        after_wal_bytes: after.map(|snapshot| snapshot.wal.bytes),
        after_shm_bytes: after.map(|snapshot| snapshot.shm.bytes),
        observed_cutoff: None,
        observed_deleted_rows: 0,
        observed_delete_batches: 0,
        canary_cutoff: None,
        canary_event_deleted_rows: 0,
        canary_provider_sample_deleted_rows: 0,
        canary_shadow_gate_deleted_rows: 0,
        canary_delete_batches: 0,
        canary_ts_indexes_created: false,
        checkpoint_truncate_attempted: false,
        checkpoint_busy: None,
        checkpoint_log_frames: None,
        checkpoint_checkpointed_frames: None,
        vacuum_into: cli
            .vacuum_into
            .as_ref()
            .map(|path| path.display().to_string()),
        vacuum_attempted: false,
        maintenance_outcome: outcome.to_string(),
        reason: reason.to_string(),
        service_safe_next_action: action.to_string(),
        error,
    }
}

fn failed_report(
    cli: &Cli,
    db_path: Option<&Path>,
    before: Option<&RuntimeDbFiles>,
    service: Option<&ServiceState>,
    error: Option<String>,
) -> RuntimeSqliteRetentionReport {
    base_report(
        cli,
        db_path,
        before,
        None,
        service,
        OUTCOME_FAILED_UNPROVEN,
        "runtime_sqlite_retention_maintenance_unproven",
        "fix the reported error before retrying retention maintenance",
        error,
    )
}

trait ReportCutoffs {
    fn with_cutoffs(
        self,
        observed_cutoff: chrono::DateTime<Utc>,
        canary_cutoff: chrono::DateTime<Utc>,
    ) -> Self;
}

impl ReportCutoffs for RuntimeSqliteRetentionReport {
    fn with_cutoffs(
        mut self,
        observed_cutoff: chrono::DateTime<Utc>,
        canary_cutoff: chrono::DateTime<Utc>,
    ) -> Self {
        self.observed_cutoff = Some(observed_cutoff.to_rfc3339());
        self.canary_cutoff = Some(canary_cutoff.to_rfc3339());
        self
    }
}
