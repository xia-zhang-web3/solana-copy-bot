#[path = "export/build.rs"]
mod build;
#[path = "export/io.rs"]
mod io;
#[path = "export/json.rs"]
mod json;

use std::{path::PathBuf, time::Duration};

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;

pub use io::InputReportStatus;

const EXECUTION_REPORT: &str = "execution_canary_quote_pnl.json";
const DISCOVERY_REPORT: &str = "discovery_v2_status.json";
const WAL_REPORT: &str = "runtime_sqlite_wal_pressure.json";
const STORAGE_CAPACITY_REPORT: &str = "storage_capacity.json";
const STRATEGY_REPORT: &str = "strategy.json";

#[derive(Debug, Clone)]
pub struct ExportOptions {
    pub input_dir: PathBuf,
    pub output_dir: PathBuf,
    pub candidate_floor: u64,
    pub max_input_age: Duration,
}

#[derive(Debug, Serialize)]
pub struct ExportReport {
    pub generated_at: DateTime<Utc>,
    pub input_dir: String,
    pub output_dir: String,
    pub candidate_floor: u64,
    pub snapshots_written: Vec<String>,
    pub inputs: Vec<InputReportStatus>,
}

pub fn export_snapshots(options: ExportOptions) -> Result<ExportReport> {
    std::fs::create_dir_all(&options.output_dir)?;

    let execution = io::load_report(&options.input_dir, EXECUTION_REPORT, options.max_input_age);
    let discovery = io::load_report(&options.input_dir, DISCOVERY_REPORT, options.max_input_age);
    let wal = io::load_report(&options.input_dir, WAL_REPORT, options.max_input_age);
    let capacity = io::load_report(
        &options.input_dir,
        STORAGE_CAPACITY_REPORT,
        options.max_input_age,
    );
    let strategy = io::load_report(&options.input_dir, STRATEGY_REPORT, options.max_input_age);

    let generated_at = Utc::now();
    let snapshots = vec![
        (
            "overview",
            build::overview_snapshot(&execution, &discovery, &wal, &capacity, &options),
        ),
        ("execution", build::execution_snapshot(&execution)),
        (
            "discovery",
            build::discovery_snapshot(&discovery, options.candidate_floor),
        ),
        ("storage", build::storage_snapshot(&wal, &capacity)),
        ("strategy", build::strategy_snapshot(&strategy, &execution)),
        (
            "alerts",
            build::alerts_snapshot(&execution, &discovery, &wal, &capacity, &options),
        ),
        (
            "reports",
            build::reports_snapshot(&[&execution, &discovery, &wal, &capacity, &strategy]),
        ),
    ];

    let mut snapshots_written = Vec::new();
    for (name, (stale, data)) in snapshots {
        io::write_snapshot(&options.output_dir, name, generated_at, stale, data)?;
        snapshots_written.push(format!("{name}.json"));
    }

    Ok(ExportReport {
        generated_at,
        input_dir: options.input_dir.display().to_string(),
        output_dir: options.output_dir.display().to_string(),
        candidate_floor: options.candidate_floor,
        snapshots_written,
        inputs: vec![
            execution.status,
            discovery.status,
            wal.status,
            capacity.status,
            strategy.status,
        ],
    })
}

pub fn parse_export_args<I>(args: I) -> Result<ExportOptions>
where
    I: IntoIterator<Item = String>,
{
    let mut input_dir = None;
    let mut output_dir = None;
    let mut candidate_floor = 8;
    let mut max_input_age_secs = 15 * 60;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--input-dir" => input_dir = Some(PathBuf::from(next_arg(&mut iter, "--input-dir")?)),
            "--output-dir" => {
                output_dir = Some(PathBuf::from(next_arg(&mut iter, "--output-dir")?))
            }
            "--candidate-floor" => {
                candidate_floor = next_arg(&mut iter, "--candidate-floor")?.parse()?;
            }
            "--max-input-age-secs" => {
                max_input_age_secs = next_arg(&mut iter, "--max-input-age-secs")?.parse()?;
            }
            "--help" | "-h" => bail!(usage()),
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(ExportOptions {
        input_dir: input_dir.ok_or_else(|| anyhow!("missing --input-dir"))?,
        output_dir: output_dir.ok_or_else(|| anyhow!("missing --output-dir"))?,
        candidate_floor,
        max_input_age: Duration::from_secs(max_input_age_secs),
    })
}

fn next_arg(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("missing value for {flag}"))
}

pub fn usage() -> &'static str {
    "usage: copybot_ops_dashboard_snapshot_export --input-dir <operator-json-dir> --output-dir <dashboard-report-dir> [--candidate-floor <n>] [--max-input-age-secs <n>]"
}
