use crate::track_b_entry_quote_report_db::{load_entry_quote_outcomes, open_read_only_db};
use crate::track_b_entry_quote_report_summary::summarize_track_b;
use crate::track_b_entry_quote_report_types::TrackBEntryQuoteSummary;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use serde::Serialize;
use std::env;
use std::path::PathBuf;

const REASON_OK: &str = "track_b_entry_quote_report_loaded";
const REASON_ERROR: &str = "track_b_entry_quote_report_error";
const DEFAULT_SINCE_HOURS: i64 = 168;
const MAX_SINCE_HOURS: i64 = 720;
const DEFAULT_LIMIT: u32 = 2000;
const MAX_LIMIT: u32 = 5000;
const DEFAULT_CLOSE_MATCH_LIMIT: u32 = 16;
const MAX_CLOSE_MATCH_LIMIT: u32 = 128;

#[derive(Debug, Clone, PartialEq)]
pub struct Cli {
    pub config_path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
    pub json: bool,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub since_hours: i64,
    pub limit: u32,
    pub close_match_limit: u32,
}

#[derive(Debug, Serialize)]
pub struct TrackBEntryQuoteReport {
    pub as_of: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub params: Option<ReportParams>,
    pub summary: Option<TrackBEntryQuoteSummary>,
}

#[derive(Debug, Serialize)]
pub struct ReportParams {
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub limit: u32,
    pub close_match_limit: u32,
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
            limit: DEFAULT_LIMIT,
            close_match_limit: DEFAULT_CLOSE_MATCH_LIMIT,
        }
    }
}

impl TrackBEntryQuoteReport {
    fn failed(as_of: DateTime<Utc>, error: impl Into<String>) -> Self {
        Self {
            as_of,
            reason_class: REASON_ERROR.to_string(),
            error: Some(error.into()),
            params: None,
            summary: None,
        }
    }

    fn exit_code(&self) -> i32 {
        i32::from(self.reason_class != REASON_OK)
    }
}

pub fn run_from_env() -> i32 {
    let as_of = Utc::now();
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(cli) if !cli.json => TrackBEntryQuoteReport::failed(as_of, "--json is required"),
        Ok(cli) => build_report(cli, as_of),
        Err(error) => TrackBEntryQuoteReport::failed(as_of, error.to_string()),
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("Track-B report must serialize")
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
                cli.since_hours = parse_bounded_i64(
                    &next_value(&mut iter, &arg)?,
                    "--since-hours",
                    1,
                    MAX_SINCE_HOURS,
                )?
            }
            "--limit" => {
                cli.limit =
                    parse_bounded_u32(&next_value(&mut iter, &arg)?, "--limit", 1, MAX_LIMIT)?
            }
            "--close-match-limit" => {
                cli.close_match_limit = parse_bounded_u32(
                    &next_value(&mut iter, &arg)?,
                    "--close-match-limit",
                    1,
                    MAX_CLOSE_MATCH_LIMIT,
                )?
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

pub fn build_report(cli: Cli, as_of: DateTime<Utc>) -> TrackBEntryQuoteReport {
    match build_report_result(cli, as_of) {
        Ok(report) => report,
        Err(error) => TrackBEntryQuoteReport::failed(as_of, error.to_string()),
    }
}

fn build_report_result(cli: Cli, as_of: DateTime<Utc>) -> Result<TrackBEntryQuoteReport> {
    let db_path = load_db_path(&cli)?;
    let until = cli.until.unwrap_or(as_of);
    let since = cli
        .since
        .unwrap_or(until - Duration::hours(cli.since_hours));
    if until <= since {
        anyhow::bail!("--until must be after --since");
    }
    let conn = open_read_only_db(&db_path)
        .with_context(|| format!("failed opening db {}", db_path.display()))?;
    let outcomes =
        load_entry_quote_outcomes(&conn, since, until, cli.limit, cli.close_match_limit)?;
    Ok(TrackBEntryQuoteReport {
        as_of,
        reason_class: REASON_OK.to_string(),
        error: None,
        params: Some(ReportParams {
            since,
            until,
            limit: cli.limit,
            close_match_limit: cli.close_match_limit,
        }),
        summary: Some(summarize_track_b(outcomes, cli.close_match_limit)),
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

fn parse_ts(raw: &str, flag: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag}: {raw}"))
}

fn parse_bounded_i64(raw: &str, flag: &str, min: i64, max: i64) -> Result<i64> {
    let value = raw
        .parse::<i64>()
        .with_context(|| format!("invalid {flag}: {raw}"))?;
    if !(min..=max).contains(&value) {
        anyhow::bail!("{flag} must be between {min} and {max}");
    }
    Ok(value)
}

fn parse_bounded_u32(raw: &str, flag: &str, min: u32, max: u32) -> Result<u32> {
    let value = raw
        .parse::<u32>()
        .with_context(|| format!("invalid {flag}: {raw}"))?;
    if !(min..=max).contains(&value) {
        anyhow::bail!("{flag} must be between {min} and {max}");
    }
    Ok(value)
}
