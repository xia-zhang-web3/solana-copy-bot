use crate::universe_de_risk_report_db::{load_sol_leg_rows, open_read_only_db, QuerySafety};
use crate::universe_de_risk_report_summary::{
    summarize_universe, SummaryThresholds, UniverseDeRiskSummary,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use serde::Serialize;
use std::env;
use std::path::PathBuf;
use std::time::{Duration as StdDuration, Instant};

const REASON_OK: &str = "universe_de_risk_report_loaded";
const REASON_ERROR: &str = "universe_de_risk_report_error";
const DEFAULT_SINCE_HOURS: i64 = 72;
const MAX_SINCE_HOURS: i64 = 168;
const DEFAULT_MAX_ROWS: usize = 1_000_000;
const MAX_ROWS: usize = 5_000_000;
const DEFAULT_TOP_TOKENS_LIMIT: usize = 25;
const MAX_TOP_TOKENS_LIMIT: usize = 200;
const DEFAULT_MIN_WALLET_TRADES: u64 = 5;
const DEFAULT_MIN_WALLET_HOLD_SECONDS: i64 = 900;
const DEFAULT_FRESH_HOURS: i64 = 6;
const DEFAULT_DEADLINE_SECONDS: u64 = 30;

#[derive(Debug, Clone, PartialEq)]
pub struct Cli {
    pub config_path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
    pub json: bool,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub since_hours: i64,
    pub max_rows: usize,
    pub top_tokens_limit: usize,
    pub min_wallet_trades: u64,
    pub min_wallet_hold_seconds: i64,
    pub fresh_hours: i64,
    pub deadline_seconds: u64,
}

#[derive(Debug, Serialize)]
pub struct UniverseDeRiskReport {
    pub as_of: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub params: Option<ReportParams>,
    pub safety: Option<QuerySafety>,
    pub summary: Option<UniverseDeRiskSummary>,
}

#[derive(Debug, Serialize)]
pub struct ReportParams {
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub since_hours: i64,
    pub max_rows: usize,
    pub top_tokens_limit: usize,
    pub min_wallet_trades: u64,
    pub min_wallet_hold_seconds: i64,
    pub fresh_hours: i64,
    pub fresh_since: DateTime<Utc>,
    pub deadline_seconds: u64,
}

impl UniverseDeRiskReport {
    fn failed(as_of: DateTime<Utc>, error: impl Into<String>) -> Self {
        Self {
            as_of,
            reason_class: REASON_ERROR.to_string(),
            error: Some(error.into()),
            params: None,
            safety: None,
            summary: None,
        }
    }

    fn exit_code(&self) -> i32 {
        i32::from(self.reason_class != REASON_OK)
    }
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            config_path: None,
            db_path: None,
            json: false,
            since: None,
            until: None,
            since_hours: DEFAULT_SINCE_HOURS,
            max_rows: DEFAULT_MAX_ROWS,
            top_tokens_limit: DEFAULT_TOP_TOKENS_LIMIT,
            min_wallet_trades: DEFAULT_MIN_WALLET_TRADES,
            min_wallet_hold_seconds: DEFAULT_MIN_WALLET_HOLD_SECONDS,
            fresh_hours: DEFAULT_FRESH_HOURS,
            deadline_seconds: DEFAULT_DEADLINE_SECONDS,
        }
    }
}

pub fn run_from_env() -> i32 {
    let as_of = Utc::now();
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(cli) if !cli.json => UniverseDeRiskReport::failed(as_of, "--json is required"),
        Ok(cli) => build_report(cli, as_of),
        Err(error) => UniverseDeRiskReport::failed(as_of, error.to_string()),
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("universe de-risk report must serialize")
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
            "--since" => cli.since = Some(parse_ts(&next_value(&mut iter, &arg)?, "--since")?),
            "--until" => cli.until = Some(parse_ts(&next_value(&mut iter, &arg)?, "--until")?),
            "--since-hours" => {
                cli.since_hours =
                    parse_bounded_i64(&next_value(&mut iter, &arg)?, &arg, 1, MAX_SINCE_HOURS)?
            }
            "--max-rows" => {
                cli.max_rows =
                    parse_bounded_usize(&next_value(&mut iter, &arg)?, &arg, 1, MAX_ROWS)?
            }
            "--top-tokens-limit" => {
                cli.top_tokens_limit = parse_bounded_usize(
                    &next_value(&mut iter, &arg)?,
                    &arg,
                    1,
                    MAX_TOP_TOKENS_LIMIT,
                )?
            }
            "--min-wallet-trades" => {
                cli.min_wallet_trades =
                    parse_bounded_u64(&next_value(&mut iter, &arg)?, &arg, 1, 1000)?
            }
            "--min-wallet-hold-seconds" => {
                cli.min_wallet_hold_seconds =
                    parse_bounded_i64(&next_value(&mut iter, &arg)?, &arg, 1, 604_800)?
            }
            "--fresh-hours" => {
                cli.fresh_hours =
                    parse_bounded_i64(&next_value(&mut iter, &arg)?, &arg, 1, MAX_SINCE_HOURS)?
            }
            "--deadline-seconds" => {
                cli.deadline_seconds =
                    parse_bounded_u64(&next_value(&mut iter, &arg)?, &arg, 1, 300)?
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

pub fn build_report(cli: Cli, as_of: DateTime<Utc>) -> UniverseDeRiskReport {
    match build_report_result(cli, as_of) {
        Ok(report) => report,
        Err(error) => UniverseDeRiskReport::failed(as_of, error.to_string()),
    }
}

fn build_report_result(cli: Cli, as_of: DateTime<Utc>) -> Result<UniverseDeRiskReport> {
    let db_path = load_db_path(&cli)?;
    let until = cli.until.unwrap_or(as_of);
    let since = cli
        .since
        .unwrap_or(until - Duration::hours(cli.since_hours));
    if until <= since {
        anyhow::bail!("--until must be after --since");
    }
    let fresh_since = until - Duration::hours(cli.fresh_hours);
    let deadline = Instant::now() + StdDuration::from_secs(cli.deadline_seconds);
    let conn = open_read_only_db(&db_path)
        .with_context(|| format!("failed opening db {}", db_path.display()))?;
    let scan = load_sol_leg_rows(&conn, since, until, cli.max_rows, deadline)?;
    let safety = scan.safety;
    let summary = summarize_universe(
        scan.rows,
        SummaryThresholds {
            min_wallet_trades: cli.min_wallet_trades,
            min_wallet_hold_seconds: cli.min_wallet_hold_seconds,
            fresh_since,
            top_tokens_limit: cli.top_tokens_limit,
        },
    );
    Ok(UniverseDeRiskReport {
        as_of,
        reason_class: REASON_OK.to_string(),
        error: None,
        params: Some(ReportParams {
            since,
            until,
            since_hours: cli.since_hours,
            max_rows: cli.max_rows,
            top_tokens_limit: cli.top_tokens_limit,
            min_wallet_trades: cli.min_wallet_trades,
            min_wallet_hold_seconds: cli.min_wallet_hold_seconds,
            fresh_hours: cli.fresh_hours,
            fresh_since,
            deadline_seconds: cli.deadline_seconds,
        }),
        safety: Some(safety),
        summary: Some(summary),
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

fn parse_ts(raw: &str, label: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("invalid {label}: {raw}"))
}

fn parse_bounded_i64(raw: &str, label: &str, min: i64, max: i64) -> Result<i64> {
    let value = raw
        .parse::<i64>()
        .with_context(|| format!("invalid {label}: {raw}"))?;
    if !(min..=max).contains(&value) {
        anyhow::bail!("{label} must be between {min} and {max}");
    }
    Ok(value)
}

fn parse_bounded_u64(raw: &str, label: &str, min: u64, max: u64) -> Result<u64> {
    let value = raw
        .parse::<u64>()
        .with_context(|| format!("invalid {label}: {raw}"))?;
    if !(min..=max).contains(&value) {
        anyhow::bail!("{label} must be between {min} and {max}");
    }
    Ok(value)
}

fn parse_bounded_usize(raw: &str, label: &str, min: usize, max: usize) -> Result<usize> {
    let value = raw
        .parse::<usize>()
        .with_context(|| format!("invalid {label}: {raw}"))?;
    if !(min..=max).contains(&value) {
        anyhow::bail!("{label} must be between {min} and {max}");
    }
    Ok(value)
}
