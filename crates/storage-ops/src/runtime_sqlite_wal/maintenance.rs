use anyhow::{anyhow, bail, Context, Result};
use copybot_config::load_from_path;
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::checkpoint::{run_sqlite_wal_checkpoint_truncate, CheckpointFailure, CheckpointResult};
#[cfg(test)]
use super::common::sqlite_sidecar_path;
use super::common::{
    compact_error, inspect_runtime_sqlite_files, resolve_db_path, FileMetadataSnapshot,
    RuntimeSqliteFilesSnapshot,
};

include!("maintenance_cli.rs");
include!("maintenance_report.rs");
include!("maintenance_systemctl.rs");

pub fn main_entry() {
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(Some(cli)) => run_with_hooks(
            &cli,
            load_service_state_from_systemctl,
            run_sqlite_wal_checkpoint_truncate,
        ),
        Ok(None) => {
            println!("{USAGE}");
            return;
        }
        Err(error) => failed_unproven_report(
            &Cli::default_for_error(),
            None,
            None,
            None,
            Some(compact_error(error)),
        ),
    };

    if report_is_json_requested(&report) {
        println!(
            "{}",
            serde_json::to_string(&report).expect("WAL maintenance report must serialize")
        );
    } else {
        println!(
            "maintenance_outcome={} reason={} production_green=false",
            report.maintenance_outcome, report.reason
        );
    }
    std::process::exit(report.exit_code());
}

fn run_with_hooks<ServiceProbe, Checkpoint>(
    cli: &Cli,
    service_probe: ServiceProbe,
    checkpoint: Checkpoint,
) -> RuntimeSqliteWalMaintenanceReport
where
    ServiceProbe: Fn(&str) -> Result<ServiceState>,
    Checkpoint: Fn(&Path, u64) -> Result<CheckpointResult, CheckpointFailure>,
{
    if !cli.json {
        return failed_unproven_report(
            cli,
            None,
            None,
            None,
            Some("runtime_sqlite_wal_maintenance_json_required".to_string()),
        );
    }
    if let Err(error) = validate_cli(cli) {
        return failed_unproven_report(cli, None, None, None, Some(compact_error(error)));
    }

    let db_path = match resolve_runtime_db_path(cli) {
        Ok(path) => path,
        Err(error) => {
            return failed_unproven_report(cli, None, None, None, Some(compact_error(error)));
        }
    };
    let before = match inspect_runtime_sqlite_files(&db_path) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return failed_unproven_report(
                cli,
                Some(db_path),
                None,
                None,
                Some(compact_error(error)),
            );
        }
    };
    let service = match service_probe(&cli.service_name) {
        Ok(service) => service,
        Err(error) => {
            return failed_unproven_report(
                cli,
                Some(db_path),
                Some(&before),
                None,
                Some(compact_error(error)),
            );
        }
    };

    if !service.service_inactive_for_maintenance() && !cli.allow_service_active {
        let reason = service.maintenance_block_reason();
        return build_report(
            cli,
            Some(&before),
            None,
            Some(&service),
            false,
            None,
            OUTCOME_FAILED_SERVICE_ACTIVE,
            reason,
            ACTION_SERVICE_ACTIVE,
            None,
        );
    }

    if before.wal.bytes < cli.min_wal_bytes {
        return build_report(
            cli,
            Some(&before),
            None,
            Some(&service),
            false,
            None,
            OUTCOME_SKIPPED_NOT_NEEDED,
            REASON_NOT_NEEDED,
            ACTION_NOT_NEEDED,
            None,
        );
    }

    if cli.dry_run {
        return build_report(
            cli,
            Some(&before),
            None,
            Some(&service),
            false,
            None,
            OUTCOME_SKIPPED_DRY_RUN,
            REASON_DRY_RUN,
            ACTION_DRY_RUN,
            None,
        );
    }

    match checkpoint(&db_path, cli.timeout_seconds) {
        Ok(result) if result.busy != 0 => build_report(
            cli,
            Some(&before),
            inspect_runtime_sqlite_files(&db_path).ok().as_ref(),
            Some(&service),
            true,
            Some(result),
            OUTCOME_FAILED_CHECKPOINT_BUSY,
            REASON_CHECKPOINT_BUSY,
            ACTION_BUSY,
            None,
        ),
        Ok(result) => {
            let after = match inspect_runtime_sqlite_files(&db_path) {
                Ok(after) => after,
                Err(error) => {
                    return failed_unproven_report(
                        cli,
                        Some(db_path),
                        Some(&before),
                        Some(&service),
                        Some(compact_error(error)),
                    );
                }
            };
            build_report(
                cli,
                Some(&before),
                Some(&after),
                Some(&service),
                true,
                Some(result),
                OUTCOME_COMPLETED,
                REASON_COMPLETED,
                ACTION_COMPLETED,
                None,
            )
        }
        Err(CheckpointFailure::Timeout(error)) => build_report(
            cli,
            Some(&before),
            inspect_runtime_sqlite_files(&db_path).ok().as_ref(),
            Some(&service),
            true,
            None,
            OUTCOME_FAILED_TIMEOUT,
            REASON_TIMEOUT,
            ACTION_TIMEOUT,
            Some(error),
        ),
        Err(CheckpointFailure::Unproven(error)) => failed_unproven_report(
            cli,
            Some(db_path),
            Some(&before),
            Some(&service),
            Some(error),
        ),
    }
}

#[cfg(test)]
#[path = "maintenance_tests.rs"]
mod maintenance_tests;
