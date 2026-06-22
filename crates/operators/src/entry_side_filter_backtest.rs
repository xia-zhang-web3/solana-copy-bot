use crate::entry_side_filter_backtest_db::{
    latest_leader_buy_before, load_closed_trades, load_history_trades, open_read_only_db,
    token_first_seen_before,
};
use crate::entry_side_filter_backtest_summary::{
    enrich_trade, summarize_entry_side_filters, BacktestSummary, SummaryParams,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use serde::Serialize;
use std::env;
use std::path::PathBuf;

const REASON_OK: &str = "entry_side_filter_backtest_loaded";
const REASON_ERROR: &str = "entry_side_filter_backtest_error";
const DEFAULT_SINCE_HOURS: i64 = 168;
const MAX_SINCE_HOURS: i64 = 720;
const DEFAULT_LIMIT: u32 = 500;
const MAX_LIMIT: u32 = 5000;
const DEFAULT_HISTORY_HOURS: i64 = 168;
const MAX_HISTORY_HOURS: i64 = 720;
const DEFAULT_HISTORY_LIMIT: u32 = 20_000;
const MAX_HISTORY_LIMIT: u32 = 100_000;
const DEFAULT_MIN_WALLET_HISTORY_CLOSES: u64 = 5;
const DEFAULT_TAIL_LOSS_SOL: f64 = -0.05;

#[derive(Debug, Clone, PartialEq)]
pub struct Cli {
    pub config_path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
    pub json: bool,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub since_hours: i64,
    pub limit: u32,
    pub history_hours: i64,
    pub history_limit: u32,
    pub min_wallet_history_closes: u64,
    pub tail_loss_sol: f64,
    pub wallet_rug_rate_thresholds: Vec<f64>,
    pub wallet_tail_rate_thresholds: Vec<f64>,
    pub token_age_minutes: Vec<i64>,
    pub leader_lag_seconds: Vec<i64>,
}

#[derive(Debug, Serialize)]
pub struct EntrySideFilterBacktestReport {
    pub as_of: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub params: Option<ReportParams>,
    pub summary: Option<BacktestSummary>,
}

#[derive(Debug, Serialize)]
pub struct ReportParams {
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub limit: u32,
    pub history_hours: i64,
    pub history_limit: u32,
    pub min_wallet_history_closes: u64,
    pub tail_loss_sol: f64,
    pub wallet_rug_rate_thresholds: Vec<f64>,
    pub wallet_tail_rate_thresholds: Vec<f64>,
    pub token_age_minutes: Vec<i64>,
    pub leader_lag_seconds: Vec<i64>,
}

impl EntrySideFilterBacktestReport {
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
        Ok(cli) if !cli.json => EntrySideFilterBacktestReport::failed(as_of, "--json is required"),
        Ok(cli) => build_report(cli, as_of),
        Err(error) => EntrySideFilterBacktestReport::failed(as_of, error.to_string()),
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("entry side backtest report must serialize")
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
            "--since-hours" => cli.since_hours = parse_hours(&next_value(&mut iter, &arg)?)?,
            "--limit" => cli.limit = parse_limit(&next_value(&mut iter, &arg)?)?,
            "--history-hours" => {
                cli.history_hours = parse_bounded_i64(
                    &next_value(&mut iter, &arg)?,
                    "--history-hours",
                    1,
                    MAX_HISTORY_HOURS,
                )?
            }
            "--history-limit" => {
                cli.history_limit = parse_bounded_u32(
                    &next_value(&mut iter, &arg)?,
                    "--history-limit",
                    1,
                    MAX_HISTORY_LIMIT,
                )?
            }
            "--min-wallet-history-closes" => {
                cli.min_wallet_history_closes =
                    parse_bounded_u64(&next_value(&mut iter, &arg)?, &arg, 1, 100)?
            }
            "--tail-loss-sol" => {
                cli.tail_loss_sol = parse_sol(&next_value(&mut iter, &arg)?, &arg)?
            }
            "--wallet-rug-rate-thresholds" => {
                cli.wallet_rug_rate_thresholds = parse_rates(&next_value(&mut iter, &arg)?)?
            }
            "--wallet-tail-rate-thresholds" => {
                cli.wallet_tail_rate_thresholds = parse_rates(&next_value(&mut iter, &arg)?)?
            }
            "--token-age-minutes" => {
                cli.token_age_minutes = parse_i64_list(&next_value(&mut iter, &arg)?, 1, 1440)?
            }
            "--leader-lag-seconds" => {
                cli.leader_lag_seconds = parse_i64_list(&next_value(&mut iter, &arg)?, 1, 3600)?
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
            until: None,
            since_hours: DEFAULT_SINCE_HOURS,
            limit: DEFAULT_LIMIT,
            history_hours: DEFAULT_HISTORY_HOURS,
            history_limit: DEFAULT_HISTORY_LIMIT,
            min_wallet_history_closes: DEFAULT_MIN_WALLET_HISTORY_CLOSES,
            tail_loss_sol: DEFAULT_TAIL_LOSS_SOL,
            wallet_rug_rate_thresholds: vec![0.10, 0.20, 0.30],
            wallet_tail_rate_thresholds: vec![0.20, 0.30, 0.40],
            token_age_minutes: vec![5, 15, 30, 60],
            leader_lag_seconds: vec![30, 60, 120],
        }
    }
}

pub fn build_report(cli: Cli, as_of: DateTime<Utc>) -> EntrySideFilterBacktestReport {
    match build_report_result(cli, as_of) {
        Ok(report) => report,
        Err(error) => EntrySideFilterBacktestReport::failed(as_of, error.to_string()),
    }
}

fn build_report_result(cli: Cli, as_of: DateTime<Utc>) -> Result<EntrySideFilterBacktestReport> {
    let db_path = load_db_path(&cli)?;
    let until = cli.until.unwrap_or(as_of);
    let since = cli
        .since
        .unwrap_or(until - Duration::hours(cli.since_hours));
    if until <= since {
        anyhow::bail!("--until must be after --since");
    }
    let conn = open_read_only_db(&db_path)
        .with_context(|| format!("failed to open db {}", db_path.display()))?;
    let trades = load_closed_trades(&conn, since, until, cli.limit)?;
    let min_opened = trades
        .iter()
        .map(|trade| trade.opened_ts)
        .min()
        .unwrap_or(since);
    let history_since = min_opened - Duration::hours(cli.history_hours);
    let history = load_history_trades(&conn, history_since, until, cli.history_limit)?;
    let history_window = Duration::hours(cli.history_hours);
    let params = summary_params(&cli);
    let mut enriched = Vec::with_capacity(trades.len());
    for trade in trades {
        let token_first_seen = token_first_seen_before(&conn, &trade.token, trade.opened_ts)?;
        let leader_buy =
            latest_leader_buy_before(&conn, &trade.wallet_id, &trade.token, trade.opened_ts)?;
        enriched.push(enrich_trade(
            trade,
            &history,
            token_first_seen,
            leader_buy,
            history_window,
            cli.tail_loss_sol,
        ));
    }
    Ok(EntrySideFilterBacktestReport {
        as_of,
        reason_class: REASON_OK.to_string(),
        error: None,
        params: Some(report_params(&cli, since, until)),
        summary: Some(summarize_entry_side_filters(&enriched, &params)),
    })
}

fn summary_params(cli: &Cli) -> SummaryParams {
    SummaryParams {
        min_wallet_history_closes: cli.min_wallet_history_closes,
        tail_loss_sol: cli.tail_loss_sol,
        wallet_rug_rate_thresholds: cli.wallet_rug_rate_thresholds.clone(),
        wallet_tail_rate_thresholds: cli.wallet_tail_rate_thresholds.clone(),
        token_age_minutes: cli.token_age_minutes.clone(),
        leader_lag_seconds: cli.leader_lag_seconds.clone(),
    }
}

fn report_params(cli: &Cli, since: DateTime<Utc>, until: DateTime<Utc>) -> ReportParams {
    ReportParams {
        since,
        until,
        limit: cli.limit,
        history_hours: cli.history_hours,
        history_limit: cli.history_limit,
        min_wallet_history_closes: cli.min_wallet_history_closes,
        tail_loss_sol: cli.tail_loss_sol,
        wallet_rug_rate_thresholds: cli.wallet_rug_rate_thresholds.clone(),
        wallet_tail_rate_thresholds: cli.wallet_tail_rate_thresholds.clone(),
        token_age_minutes: cli.token_age_minutes.clone(),
        leader_lag_seconds: cli.leader_lag_seconds.clone(),
    }
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

fn parse_hours(raw: &str) -> Result<i64> {
    parse_bounded_i64(raw, "--since-hours", 1, MAX_SINCE_HOURS)
}

fn parse_limit(raw: &str) -> Result<u32> {
    parse_bounded_u32(raw, "--limit", 1, MAX_LIMIT)
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

fn parse_sol(raw: &str, flag: &str) -> Result<f64> {
    let value = raw
        .parse::<f64>()
        .with_context(|| format!("invalid {flag}: {raw}"))?;
    if !value.is_finite() || value > 0.0 || value < -10.0 {
        anyhow::bail!("{flag} must be finite and between -10 and 0");
    }
    Ok(value)
}

fn parse_rates(raw: &str) -> Result<Vec<f64>> {
    let values = parse_f64_list(raw)?;
    if values.iter().any(|value| *value < 0.0 || *value > 1.0) {
        anyhow::bail!("rates must be between 0 and 1");
    }
    Ok(values)
}

fn parse_f64_list(raw: &str) -> Result<Vec<f64>> {
    let mut values = Vec::new();
    for part in raw.split(',') {
        let value = part.trim().parse::<f64>()?;
        if !value.is_finite() {
            anyhow::bail!("list values must be finite");
        }
        values.push(value);
    }
    if values.is_empty() {
        anyhow::bail!("list cannot be empty");
    }
    Ok(values)
}

fn parse_i64_list(raw: &str, min: i64, max: i64) -> Result<Vec<i64>> {
    let mut values = Vec::new();
    for part in raw.split(',') {
        values.push(parse_bounded_i64(part.trim(), "list value", min, max)?);
    }
    if values.is_empty() {
        anyhow::bail!("list cannot be empty");
    }
    Ok(values)
}
