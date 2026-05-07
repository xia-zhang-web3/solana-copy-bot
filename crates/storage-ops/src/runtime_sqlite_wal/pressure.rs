#[cfg(test)]
use super::common::sqlite_sidecar_path;
#[cfg(test)]
use super::common::{compact_error, inspect_runtime_sqlite_files};
#[cfg(test)]
use anyhow::{Context, Result};
#[cfg(test)]
use std::env;

#[path = "pressure_cli.rs"]
mod cli;
#[path = "pressure_report.rs"]
mod report;
#[path = "pressure_types.rs"]
mod types;

pub use self::cli::main_entry;

#[cfg(test)]
use self::{
    cli::parse_args_from,
    report::{build_report_from_snapshot, unproven_report},
    types::{
        Cli, RuntimeSqliteWalPressureReport, WalPressureLevel, ACTION_CRITICAL, ACTION_LARGE,
        REASON_CRITICAL, REASON_LARGE, REASON_NONE, REASON_UNPROVEN_METADATA,
    },
};

#[cfg(test)]
#[path = "pressure_tests.rs"]
mod pressure_tests;
