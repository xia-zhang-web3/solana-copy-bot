use super::{
    report::{report_should_render_json, run, unproven_report},
    types::{Cli, DEFAULT_CRITICAL_WAL_THRESHOLD_BYTES, DEFAULT_LARGE_WAL_THRESHOLD_BYTES, USAGE},
};
use crate::runtime_sqlite_wal::common::compact_error;
use anyhow::{anyhow, bail, Context, Result};
use std::{env, path::PathBuf};

pub fn main_entry() {
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(Some(cli)) => match run(&cli) {
            Ok(report) => report,
            Err(error) => unproven_report(None, &cli, Some(compact_error(error))),
        },
        Ok(None) => {
            println!("{USAGE}");
            return;
        }
        Err(error) => unproven_report(
            None,
            &Cli {
                config_path: PathBuf::new(),
                db_path_override: None,
                json: true,
                large_wal_threshold_bytes: DEFAULT_LARGE_WAL_THRESHOLD_BYTES,
                critical_wal_threshold_bytes: DEFAULT_CRITICAL_WAL_THRESHOLD_BYTES,
            },
            Some(compact_error(error)),
        ),
    };

    if report_should_render_json(&report) {
        println!(
            "{}",
            serde_json::to_string(&report).expect("WAL pressure report must serialize")
        );
    } else {
        println!(
            "wal_pressure_level={} wal_pressure_reason={} production_green=false",
            report.wal_pressure_level.as_str(),
            report.wal_pressure_reason
        );
    }
    std::process::exit(report.exit_code());
}

pub(super) fn parse_args_from<I>(args: I) -> Result<Option<Cli>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut db_path_override: Option<PathBuf> = None;
    let mut json = false;
    let mut large_wal_threshold_bytes = DEFAULT_LARGE_WAL_THRESHOLD_BYTES;
    let mut critical_wal_threshold_bytes = DEFAULT_CRITICAL_WAL_THRESHOLD_BYTES;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--db-path" => {
                db_path_override = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?));
            }
            "--json" => json = true,
            "--large-wal-threshold-bytes" => {
                large_wal_threshold_bytes =
                    parse_u64_arg("--large-wal-threshold-bytes", args.next())?;
            }
            "--critical-wal-threshold-bytes" => {
                critical_wal_threshold_bytes =
                    parse_u64_arg("--critical-wal-threshold-bytes", args.next())?;
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Cli {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path_override,
        json,
        large_wal_threshold_bytes,
        critical_wal_threshold_bytes,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed.to_string())
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("{flag} must be an unsigned integer; got {raw}"))
}
