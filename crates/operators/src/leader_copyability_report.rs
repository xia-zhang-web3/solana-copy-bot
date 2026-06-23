use crate::leader_copyability_report_db::{
    latest_wallet_metric_window_at_or_before, load_active_wallets, load_follower_close_facts,
    load_leader_close_facts, load_wallet_metrics_for_window, open_read_only_db, WalletMetric,
};
use crate::leader_copyability_report_summary::{
    summarize_copyability, CopyabilitySummary, WalletInputs,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::time::{Duration as StdDuration, Instant};

const REASON_OK: &str = "leader_copyability_report_loaded";
const REASON_ERROR: &str = "leader_copyability_report_error";
const DEFAULT_SINCE_HOURS: i64 = 168;
const MAX_SINCE_HOURS: i64 = 720;
const DEFAULT_LIMIT: u32 = 50;
const MAX_LIMIT: u32 = 500;
const DEFAULT_PER_WALLET_LIMIT: u32 = 50_000;
const MAX_PER_WALLET_LIMIT: u32 = 100_000;
const DEFAULT_MIN_LEADER_TRADES: u64 = 5;
const DEFAULT_MIN_FOLLOWER_TRADES: u64 = 5;
const DEFAULT_DEADLINE_SECONDS: u64 = 30;

#[derive(Debug, Clone, PartialEq)]
pub struct Cli {
    pub config_path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
    pub json: bool,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub since_hours: i64,
    pub limit: u32,
    pub per_wallet_limit: u32,
    pub min_leader_trades: u64,
    pub min_follower_trades: u64,
    pub deadline_seconds: u64,
}

#[derive(Debug, Serialize)]
pub struct LeaderCopyabilityReport {
    pub as_of: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub params: Option<ReportParams>,
    pub summary: Option<CopyabilitySummary>,
}

#[derive(Debug, Serialize)]
pub struct ReportParams {
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub active_wallet_limit: u32,
    pub per_wallet_limit: u32,
    pub min_leader_trades: u64,
    pub min_follower_trades: u64,
    pub deadline_seconds: u64,
    pub wallet_metric_window_start: Option<DateTime<Utc>>,
}

impl LeaderCopyabilityReport {
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
            per_wallet_limit: DEFAULT_PER_WALLET_LIMIT,
            min_leader_trades: DEFAULT_MIN_LEADER_TRADES,
            min_follower_trades: DEFAULT_MIN_FOLLOWER_TRADES,
            deadline_seconds: DEFAULT_DEADLINE_SECONDS,
        }
    }
}

pub fn run_from_env() -> i32 {
    let as_of = Utc::now();
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(cli) if !cli.json => LeaderCopyabilityReport::failed(as_of, "--json is required"),
        Ok(cli) => build_report(cli, as_of),
        Err(error) => LeaderCopyabilityReport::failed(as_of, error.to_string()),
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("leader copyability report must serialize")
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
            "--per-wallet-limit" => {
                cli.per_wallet_limit = parse_bounded_u32(
                    &next_value(&mut iter, &arg)?,
                    "--per-wallet-limit",
                    1,
                    MAX_PER_WALLET_LIMIT,
                )?
            }
            "--min-leader-trades" => {
                cli.min_leader_trades =
                    parse_bounded_u64(&next_value(&mut iter, &arg)?, &arg, 1, 1000)?
            }
            "--min-follower-trades" => {
                cli.min_follower_trades =
                    parse_bounded_u64(&next_value(&mut iter, &arg)?, &arg, 1, 1000)?
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

pub fn build_report(cli: Cli, as_of: DateTime<Utc>) -> LeaderCopyabilityReport {
    match build_report_result(cli, as_of) {
        Ok(report) => report,
        Err(error) => LeaderCopyabilityReport::failed(as_of, error.to_string()),
    }
}

fn build_report_result(cli: Cli, as_of: DateTime<Utc>) -> Result<LeaderCopyabilityReport> {
    let db_path = load_db_path(&cli)?;
    let until = cli.until.unwrap_or(as_of);
    let since = cli
        .since
        .unwrap_or(until - Duration::hours(cli.since_hours));
    if until <= since {
        anyhow::bail!("--until must be after --since");
    }
    let deadline = Instant::now() + StdDuration::from_secs(cli.deadline_seconds);
    let conn = open_read_only_db(&db_path)
        .with_context(|| format!("failed opening db {}", db_path.display()))?;
    let active_wallets = load_active_wallets(&conn, cli.limit)?;
    let metric_window = latest_wallet_metric_window_at_or_before(&conn, until)?;
    let metrics = match metric_window {
        Some(window_start) => metric_map(load_wallet_metrics_for_window(&conn, window_start)?),
        None => HashMap::new(),
    };
    let mut inputs = Vec::with_capacity(active_wallets.len());
    for wallet in active_wallets {
        if Instant::now() >= deadline {
            anyhow::bail!("deadline exceeded while collecting per-wallet copyability rows");
        }
        let leader_closes =
            load_leader_close_facts(&conn, &wallet.wallet_id, since, until, cli.per_wallet_limit)?;
        let follower_closes = load_follower_close_facts(
            &conn,
            &wallet.wallet_id,
            since,
            until,
            cli.per_wallet_limit,
        )?;
        inputs.push(WalletInputs {
            metric: metrics.get(&wallet.wallet_id).cloned(),
            wallet_id: wallet.wallet_id,
            leader_closes,
            follower_closes,
        });
    }
    Ok(LeaderCopyabilityReport {
        as_of,
        reason_class: REASON_OK.to_string(),
        error: None,
        params: Some(ReportParams {
            since,
            until,
            active_wallet_limit: cli.limit,
            per_wallet_limit: cli.per_wallet_limit,
            min_leader_trades: cli.min_leader_trades,
            min_follower_trades: cli.min_follower_trades,
            deadline_seconds: cli.deadline_seconds,
            wallet_metric_window_start: metric_window,
        }),
        summary: Some(summarize_copyability(
            inputs,
            cli.min_leader_trades,
            cli.min_follower_trades,
        )),
    })
}

fn metric_map(rows: Vec<WalletMetric>) -> HashMap<String, WalletMetric> {
    rows.into_iter()
        .map(|row| (row.wallet_id.clone(), row))
        .collect()
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
        .with_context(|| format!("invalid {flag} RFC3339 timestamp: {raw}"))
}

fn parse_bounded_i64(raw: &str, flag: &str, min: i64, max: i64) -> Result<i64> {
    let value = raw
        .parse::<i64>()
        .with_context(|| format!("invalid {flag}: {raw}"))?;
    if value < min || value > max {
        anyhow::bail!("{flag} must be between {min} and {max}");
    }
    Ok(value)
}

fn parse_bounded_u32(raw: &str, flag: &str, min: u32, max: u32) -> Result<u32> {
    let value = raw
        .parse::<u32>()
        .with_context(|| format!("invalid {flag}: {raw}"))?;
    if value < min || value > max {
        anyhow::bail!("{flag} must be between {min} and {max}");
    }
    Ok(value)
}

fn parse_bounded_u64(raw: &str, flag: &str, min: u64, max: u64) -> Result<u64> {
    let value = raw
        .parse::<u64>()
        .with_context(|| format!("invalid {flag}: {raw}"))?;
    if value < min || value > max {
        anyhow::bail!("{flag} must be between {min} and {max}");
    }
    Ok(value)
}
