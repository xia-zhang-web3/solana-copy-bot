use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use std::env;
use std::path::PathBuf;

pub(crate) const USAGE: &str = "usage: discovery_v2_wallet_report --config <path> \
    [--db-path <path>] [--top <n>] [--include-rejected] [--live-rebuild] \
    [--simulate-rug-filter] [--rug-filter-max-stale-terminal-rate <ratio>] \
    [--rug-filter-max-stale-terminal-pnl-sol <sol>] \
    [--rug-feedback-since <rfc3339>] [--rug-feedback-until <rfc3339>] \
    [--rug-feedback-min-closed-trades <n>] \
    [--rug-feedback-rate-threshold <ratio>] \
    [--rug-feedback-pnl-threshold-sol <sol>]";

#[derive(Debug, Clone)]
pub(crate) struct WalletReportCliConfig {
    pub(crate) config_path: PathBuf,
    pub(crate) db_path: Option<PathBuf>,
    pub(crate) top: usize,
    pub(crate) include_rejected: bool,
    pub(crate) live_rebuild: bool,
    pub(crate) simulate_rug_filter: bool,
    pub(crate) rug_filter_max_stale_terminal_rate: Option<f64>,
    pub(crate) rug_filter_max_stale_terminal_pnl_sol: Option<f64>,
    pub(crate) rug_feedback_since: Option<DateTime<Utc>>,
    pub(crate) rug_feedback_until: Option<DateTime<Utc>>,
    pub(crate) rug_feedback_min_closed_trades: Option<u32>,
    pub(crate) rug_feedback_rate_threshold: Option<f64>,
    pub(crate) rug_feedback_pnl_threshold_sol: Option<f64>,
}

pub(crate) fn parse_args() -> Result<Option<WalletReportCliConfig>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<WalletReportCliConfig>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path = None;
    let mut db_path = None;
    let mut top = 15usize;
    let mut include_rejected = false;
    let mut live_rebuild = false;
    let mut simulate_rug_filter = false;
    let mut rug_filter_max_stale_terminal_rate = None;
    let mut rug_filter_max_stale_terminal_pnl_sol = None;
    let mut rug_feedback_since = None;
    let mut rug_feedback_until = None;
    let mut rug_feedback_min_closed_trades = None;
    let mut rug_feedback_rate_threshold = None;
    let mut rug_feedback_pnl_threshold_sol = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--top" => top = parse_top(args.next())?,
            "--include-rejected" => include_rejected = true,
            "--live-rebuild" => live_rebuild = true,
            "--simulate-rug-filter" => simulate_rug_filter = true,
            "--rug-filter-max-stale-terminal-rate" => {
                rug_filter_max_stale_terminal_rate = Some(parse_ratio_arg(&arg, args.next())?)
            }
            "--rug-filter-max-stale-terminal-pnl-sol" => {
                rug_filter_max_stale_terminal_pnl_sol = Some(parse_f64_arg(&arg, args.next())?)
            }
            "--rug-feedback-since" => {
                rug_feedback_since = Some(parse_datetime_arg(&arg, args.next())?)
            }
            "--rug-feedback-until" => {
                rug_feedback_until = Some(parse_datetime_arg(&arg, args.next())?)
            }
            "--rug-feedback-min-closed-trades" => {
                rug_feedback_min_closed_trades = Some(parse_u32_arg(&arg, args.next())?)
            }
            "--rug-feedback-rate-threshold" => {
                rug_feedback_rate_threshold = Some(parse_ratio_arg(&arg, args.next())?)
            }
            "--rug-feedback-pnl-threshold-sol" => {
                rug_feedback_pnl_threshold_sol = Some(parse_f64_arg(&arg, args.next())?)
            }
            "--window-minutes"
            | "--max-tail-lag-seconds"
            | "--max-rows"
            | "--time-budget-ms"
            | "--now" => bail!(
                "{arg} is not accepted by production wallet report; use config values and wall clock"
            ),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }
    if (simulate_rug_filter
        || rug_filter_max_stale_terminal_rate.is_some()
        || rug_filter_max_stale_terminal_pnl_sol.is_some())
        && !live_rebuild
    {
        bail!("rug filter simulation requires --live-rebuild");
    }
    if (rug_filter_max_stale_terminal_rate.is_some()
        || rug_filter_max_stale_terminal_pnl_sol.is_some())
        && !simulate_rug_filter
    {
        bail!("rug threshold overrides require --simulate-rug-filter");
    }
    if rug_feedback_until.is_some() && rug_feedback_since.is_none() {
        bail!("--rug-feedback-until requires --rug-feedback-since");
    }
    if (rug_feedback_min_closed_trades.is_some()
        || rug_feedback_rate_threshold.is_some()
        || rug_feedback_pnl_threshold_sol.is_some())
        && rug_feedback_since.is_none()
    {
        bail!("rug feedback overrides require --rug-feedback-since");
    }
    if rug_feedback_since.is_some() && (live_rebuild || simulate_rug_filter) {
        bail!("rug feedback distribution cannot be combined with --live-rebuild or --simulate-rug-filter");
    }
    Ok(Some(WalletReportCliConfig {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
        top,
        include_rejected,
        live_rebuild,
        simulate_rug_filter,
        rug_filter_max_stale_terminal_rate,
        rug_filter_max_stale_terminal_pnl_sol,
        rug_feedback_since,
        rug_feedback_until,
        rug_feedback_min_closed_trades,
        rug_feedback_rate_threshold,
        rug_feedback_pnl_threshold_sol,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn parse_top(value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg("--top", value)?;
    let top = raw
        .parse::<usize>()
        .with_context(|| format!("invalid --top value: {raw}"))?;
    if top == 0 || top > 250 {
        bail!("--top must be between 1 and 250");
    }
    Ok(top)
}

fn parse_ratio_arg(flag: &str, value: Option<String>) -> Result<f64> {
    let parsed = parse_f64_arg(flag, value)?;
    if !(0.0..=1.0).contains(&parsed) {
        bail!("{flag} must be between 0 and 1");
    }
    Ok(parsed)
}

fn parse_f64_arg(flag: &str, value: Option<String>) -> Result<f64> {
    let raw = parse_string_arg(flag, value)?;
    let parsed = raw
        .parse::<f64>()
        .with_context(|| format!("invalid {flag} value: {raw}"))?;
    if !parsed.is_finite() {
        bail!("{flag} must be finite");
    }
    Ok(parsed)
}

fn parse_u32_arg(flag: &str, value: Option<String>) -> Result<u32> {
    let raw = parse_string_arg(flag, value)?;
    let parsed = raw
        .parse::<u32>()
        .with_context(|| format!("invalid {flag} value: {raw}"))?;
    if parsed == 0 {
        bail!("{flag} must be >= 1");
    }
    Ok(parsed)
}

fn parse_datetime_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .with_context(|| format!("invalid {flag} timestamp: {raw}"))
        .map(|ts| ts.with_timezone(&Utc))
}
