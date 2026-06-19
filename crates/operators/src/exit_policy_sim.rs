use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_storage_core::{
    ExecutionExitPolicySimConfig, ExecutionExitPolicySimReport, SqliteStore,
};
use serde::Serialize;
use std::env;
use std::path::PathBuf;

const REASON_OK: &str = "exit_policy_sim_loaded";
const REASON_ERROR: &str = "exit_policy_sim_error";
const DEFAULT_LIMIT: u32 = 200;
const MAX_LIMIT: u32 = 500;
const DEFAULT_SINCE_HOURS: i64 = 48;
const MAX_SINCE_HOURS: i64 = 168;
const DEFAULT_SAMPLE_WINDOW_MINUTES: i64 = 15;
const DEFAULT_MIN_SOL_NOTIONAL: f64 = 0.01;
const DEFAULT_MIN_SAMPLES: usize = 1;
const DEFAULT_MAX_SAMPLES: usize = 60;
const DEFAULT_CHECKPOINT_STEP_MINUTES: i64 = 5;
const DEFAULT_HORIZON_MINUTES: i64 = 90;

#[derive(Debug, Clone, PartialEq)]
pub struct Cli {
    pub config_path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
    pub json: bool,
    pub since: Option<DateTime<Utc>>,
    pub since_hours: i64,
    pub limit: u32,
    pub sample_window_minutes: i64,
    pub min_sol_notional: f64,
    pub min_samples: usize,
    pub max_samples_per_checkpoint: usize,
    pub checkpoint_step_minutes: i64,
    pub horizon_minutes: i64,
    pub backstop_minutes: Vec<i64>,
    pub price_collapse_ratios: Vec<f64>,
    pub activity_idle_minutes: Vec<i64>,
}

#[derive(Debug, Serialize)]
pub struct ExitPolicySimOperatorReport {
    pub as_of: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub report: Option<ExecutionExitPolicySimReport>,
}

impl ExitPolicySimOperatorReport {
    fn failed(as_of: DateTime<Utc>, error: impl Into<String>) -> Self {
        Self {
            as_of,
            reason_class: REASON_ERROR.to_string(),
            error: Some(error.into()),
            report: None,
        }
    }

    fn ok(as_of: DateTime<Utc>, report: ExecutionExitPolicySimReport) -> Self {
        Self {
            as_of,
            reason_class: REASON_OK.to_string(),
            error: None,
            report: Some(report),
        }
    }

    fn exit_code(&self) -> i32 {
        i32::from(self.reason_class != REASON_OK)
    }
}

pub fn run_from_env() -> i32 {
    let as_of = Utc::now();
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(cli) if !cli.json => ExitPolicySimOperatorReport::failed(as_of, "--json is required"),
        Ok(cli) => build_report(cli, as_of),
        Err(error) => ExitPolicySimOperatorReport::failed(as_of, error.to_string()),
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("exit policy sim report must serialize")
    );
    report.exit_code()
}

pub fn parse_args_from<I>(args: I) -> Result<Cli>
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let mut cli = Cli::default();
    let mut iter = args.into_iter().map(Into::into);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => cli.config_path = Some(PathBuf::from(next_value(&mut iter, &arg)?)),
            "--db-path" => cli.db_path = Some(PathBuf::from(next_value(&mut iter, &arg)?)),
            "--since" => cli.since = Some(parse_since(&next_value(&mut iter, &arg)?)?),
            "--since-hours" => cli.since_hours = parse_since_hours(&next_value(&mut iter, &arg)?)?,
            "--limit" => cli.limit = parse_limit(&next_value(&mut iter, &arg)?)?,
            "--sample-window-minutes" => {
                cli.sample_window_minutes = parse_minutes(&next_value(&mut iter, &arg)?, 120)?
            }
            "--min-sol-notional" => {
                cli.min_sol_notional = parse_sol_notional(&next_value(&mut iter, &arg)?)?
            }
            "--min-samples" => cli.min_samples = parse_samples(&next_value(&mut iter, &arg)?)?,
            "--max-samples-per-checkpoint" => {
                cli.max_samples_per_checkpoint = parse_samples(&next_value(&mut iter, &arg)?)?
            }
            "--checkpoint-step-minutes" => {
                cli.checkpoint_step_minutes = parse_minutes(&next_value(&mut iter, &arg)?, 30)?
            }
            "--horizon-minutes" => {
                cli.horizon_minutes = parse_minutes(&next_value(&mut iter, &arg)?, 240)?
            }
            "--backstop-minutes" => {
                cli.backstop_minutes = parse_minutes_list(&next_value(&mut iter, &arg)?, 240)?
            }
            "--price-collapse-ratios" => {
                cli.price_collapse_ratios = parse_ratio_list(&next_value(&mut iter, &arg)?)?
            }
            "--activity-idle-minutes" => {
                cli.activity_idle_minutes = parse_minutes_list(&next_value(&mut iter, &arg)?, 240)?
            }
            "--json" => cli.json = true,
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }
    if cli.config_path.is_none() && cli.db_path.is_none() {
        anyhow::bail!("either --config or --db-path is required");
    }
    Ok(cli)
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            config_path: None,
            db_path: None,
            json: false,
            since: None,
            since_hours: DEFAULT_SINCE_HOURS,
            limit: DEFAULT_LIMIT,
            sample_window_minutes: DEFAULT_SAMPLE_WINDOW_MINUTES,
            min_sol_notional: DEFAULT_MIN_SOL_NOTIONAL,
            min_samples: DEFAULT_MIN_SAMPLES,
            max_samples_per_checkpoint: DEFAULT_MAX_SAMPLES,
            checkpoint_step_minutes: DEFAULT_CHECKPOINT_STEP_MINUTES,
            horizon_minutes: DEFAULT_HORIZON_MINUTES,
            backstop_minutes: vec![30, 45],
            price_collapse_ratios: vec![0.50, 0.30, 0.20],
            activity_idle_minutes: vec![15, 30],
        }
    }
}

fn build_report(cli: Cli, as_of: DateTime<Utc>) -> ExitPolicySimOperatorReport {
    match build_report_result(cli, as_of) {
        Ok(report) => ExitPolicySimOperatorReport::ok(as_of, report),
        Err(error) => ExitPolicySimOperatorReport::failed(as_of, error.to_string()),
    }
}

fn build_report_result(cli: Cli, as_of: DateTime<Utc>) -> Result<ExecutionExitPolicySimReport> {
    let db_path = load_db_path(&cli)?;
    let store = SqliteStore::open_read_only(&db_path)
        .with_context(|| format!("failed to open db {}", db_path.display()))?;
    store.tune_for_operator_scans()?;
    let since = cli
        .since
        .unwrap_or(as_of - Duration::hours(cli.since_hours));
    store.execution_exit_policy_sim_report(ExecutionExitPolicySimConfig {
        since,
        limit: cli.limit,
        sample_window_minutes: cli.sample_window_minutes,
        min_sol_notional: cli.min_sol_notional,
        min_samples: cli.min_samples,
        max_samples_per_checkpoint: cli.max_samples_per_checkpoint,
        checkpoint_step_minutes: cli.checkpoint_step_minutes,
        horizon_minutes: cli.horizon_minutes,
        backstop_minutes: cli.backstop_minutes,
        price_collapse_ratios: cli.price_collapse_ratios,
        activity_idle_minutes: cli.activity_idle_minutes,
    })
}

fn load_db_path(cli: &Cli) -> Result<PathBuf> {
    if let Some(config_path) = &cli.config_path {
        let config = load_from_path(config_path)
            .with_context(|| format!("failed to load config {}", config_path.display()))?;
        return Ok(cli
            .db_path
            .clone()
            .unwrap_or_else(|| PathBuf::from(config.sqlite.path.clone())));
    }
    cli.db_path
        .clone()
        .ok_or_else(|| anyhow!("either --config or --db-path is required"))
}

fn next_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn parse_since(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("invalid --since RFC3339 timestamp: {raw}"))
}

fn parse_since_hours(raw: &str) -> Result<i64> {
    let value = raw
        .parse::<i64>()
        .with_context(|| format!("invalid --since-hours: {raw}"))?;
    if value <= 0 || value > MAX_SINCE_HOURS {
        anyhow::bail!("--since-hours must be between 1 and {MAX_SINCE_HOURS}");
    }
    Ok(value)
}

fn parse_limit(raw: &str) -> Result<u32> {
    let value = raw
        .parse::<u32>()
        .with_context(|| format!("invalid --limit: {raw}"))?;
    if value == 0 || value > MAX_LIMIT {
        anyhow::bail!("--limit must be between 1 and {MAX_LIMIT}");
    }
    Ok(value)
}

fn parse_minutes(raw: &str, max: i64) -> Result<i64> {
    let value = raw
        .parse::<i64>()
        .with_context(|| format!("invalid minute value: {raw}"))?;
    if value <= 0 || value > max {
        anyhow::bail!("minute value must be between 1 and {max}");
    }
    Ok(value)
}

fn parse_minutes_list(raw: &str, max: i64) -> Result<Vec<i64>> {
    parse_list(raw, |value| parse_minutes(value, max))
}

fn parse_ratio_list(raw: &str) -> Result<Vec<f64>> {
    parse_list(raw, |value| {
        let parsed = value
            .parse::<f64>()
            .with_context(|| format!("invalid ratio: {value}"))?;
        if !parsed.is_finite() || parsed <= 0.0 || parsed >= 1.0 {
            anyhow::bail!("ratio must be between 0 and 1");
        }
        Ok(parsed)
    })
}

fn parse_list<T>(raw: &str, parse: impl Fn(&str) -> Result<T>) -> Result<Vec<T>> {
    let values = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(parse)
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        anyhow::bail!("list must contain at least one value");
    }
    Ok(values)
}

fn parse_samples(raw: &str) -> Result<usize> {
    let value = raw
        .parse::<usize>()
        .with_context(|| format!("invalid sample count: {raw}"))?;
    if value == 0 || value > 200 {
        anyhow::bail!("sample count must be between 1 and 200");
    }
    Ok(value)
}

fn parse_sol_notional(raw: &str) -> Result<f64> {
    let value = raw
        .parse::<f64>()
        .with_context(|| format!("invalid SOL notional: {raw}"))?;
    if !value.is_finite() || value < 0.0 || value > 1.0 {
        anyhow::bail!("SOL notional must be between 0 and 1");
    }
    Ok(value)
}
