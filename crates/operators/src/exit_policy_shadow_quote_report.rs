use crate::exit_policy_shadow_quote_report_db::{load_diagnostic_events, open_read_only_db};
use crate::exit_policy_shadow_quote_report_summary::{
    summarize_exit_policy_shadow_quotes, BenefitReport, BenefitSummary, ConditionalSweepReport,
    CountSummary, ErrorClassCount,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use serde::Serialize;
use std::env;
use std::path::PathBuf;

const REASON_OK: &str = "exit_policy_shadow_quote_report_loaded";
const REASON_ERROR: &str = "exit_policy_shadow_quote_report_error";
const DEFAULT_HOLD_MINUTES: i64 = 30;
const DEFAULT_SINCE_HOURS: i64 = 168;
const MAX_SINCE_HOURS: i64 = 720;
const DEFAULT_LIMIT: u32 = 500;
const MAX_LIMIT: u32 = 5000;
const DEFAULT_STEADY_MAX_DELAY_MS: u64 = 120_000;
const DEFAULT_EXIT_COST_SOL: f64 = 0.002;
const DEFAULT_CLOSE_MATCH_LIMIT: u32 = 16;

#[derive(Debug, Clone, PartialEq)]
pub struct Cli {
    pub config_path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
    pub json: bool,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub since_hours: i64,
    pub limit: u32,
    pub hold_minutes: i64,
    pub steady_max_delay_ms: u64,
    pub exit_cost_sol: f64,
    pub thresholds: Vec<f64>,
    pub close_match_limit: u32,
}

#[derive(Debug, Serialize)]
pub struct ExitPolicyShadowQuoteOperatorReport {
    pub as_of: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub params: Option<ReportParams>,
    pub counts: Option<CountSummary>,
    pub benefit: Option<BenefitReport>,
    pub steady_benefit: Option<BenefitReport>,
    pub by_close_context: Vec<BenefitSummary>,
    pub by_error_class: Vec<ErrorClassCount>,
    pub conditional_sweeps: Vec<ConditionalSweepReport>,
    pub join_method: Option<JoinMethodReport>,
}

#[derive(Debug, Serialize)]
pub struct ReportParams {
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub limit: u32,
    pub hold_minutes: i64,
    pub steady_max_delay_ms: u64,
    pub exit_cost_sol: f64,
    pub thresholds: Vec<f64>,
    pub close_match_limit: u32,
}

#[derive(Debug, Serialize)]
pub struct JoinMethodReport {
    pub method: String,
    pub lot_id_in_event_id: bool,
    pub shadow_closed_trade_lot_id_available: bool,
    pub ambiguity_note: String,
}

impl ExitPolicyShadowQuoteOperatorReport {
    fn failed(as_of: DateTime<Utc>, error: impl Into<String>) -> Self {
        Self {
            as_of,
            reason_class: REASON_ERROR.to_string(),
            error: Some(error.into()),
            params: None,
            counts: None,
            benefit: None,
            steady_benefit: None,
            by_close_context: Vec::new(),
            by_error_class: Vec::new(),
            conditional_sweeps: Vec::new(),
            join_method: None,
        }
    }

    fn exit_code(&self) -> i32 {
        i32::from(self.reason_class != REASON_OK)
    }
}

pub fn run_from_env() -> i32 {
    let as_of = Utc::now();
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(cli) if !cli.json => {
            ExitPolicyShadowQuoteOperatorReport::failed(as_of, "--json is required")
        }
        Ok(cli) => build_report(cli, as_of),
        Err(error) => ExitPolicyShadowQuoteOperatorReport::failed(as_of, error.to_string()),
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("exit policy shadow quote report must serialize")
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
            "--since-hours" => cli.since_hours = parse_since_hours(&next_value(&mut iter, &arg)?)?,
            "--limit" => cli.limit = parse_limit(&next_value(&mut iter, &arg)?)?,
            "--hold-minutes" => cli.hold_minutes = parse_minutes(&next_value(&mut iter, &arg)?)?,
            "--steady-max-delay-ms" => {
                cli.steady_max_delay_ms = parse_u64(&next_value(&mut iter, &arg)?, &arg)?
            }
            "--exit-cost-sol" => {
                cli.exit_cost_sol = parse_sol_amount(&next_value(&mut iter, &arg)?, &arg)?
            }
            "--thresholds" => cli.thresholds = parse_thresholds(&next_value(&mut iter, &arg)?)?,
            "--close-match-limit" => {
                cli.close_match_limit = parse_close_match_limit(&next_value(&mut iter, &arg)?)?
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
            hold_minutes: DEFAULT_HOLD_MINUTES,
            steady_max_delay_ms: DEFAULT_STEADY_MAX_DELAY_MS,
            exit_cost_sol: DEFAULT_EXIT_COST_SOL,
            thresholds: vec![0.90, 0.80, 0.70],
            close_match_limit: DEFAULT_CLOSE_MATCH_LIMIT,
        }
    }
}

pub fn build_report(cli: Cli, as_of: DateTime<Utc>) -> ExitPolicyShadowQuoteOperatorReport {
    match build_report_result(cli, as_of) {
        Ok(report) => report,
        Err(error) => ExitPolicyShadowQuoteOperatorReport::failed(as_of, error.to_string()),
    }
}

fn build_report_result(
    cli: Cli,
    as_of: DateTime<Utc>,
) -> Result<ExitPolicyShadowQuoteOperatorReport> {
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
    let events = load_diagnostic_events(
        &conn,
        since,
        until,
        cli.limit,
        cli.hold_minutes,
        cli.close_match_limit,
    )?;
    let summary = summarize_exit_policy_shadow_quotes(
        events,
        &cli.thresholds,
        cli.exit_cost_sol,
        cli.steady_max_delay_ms,
    );

    Ok(ExitPolicyShadowQuoteOperatorReport {
        as_of,
        reason_class: REASON_OK.to_string(),
        error: None,
        params: Some(ReportParams {
            since,
            until,
            limit: cli.limit,
            hold_minutes: cli.hold_minutes,
            steady_max_delay_ms: cli.steady_max_delay_ms,
            exit_cost_sol: cli.exit_cost_sol,
            thresholds: cli.thresholds,
            close_match_limit: cli.close_match_limit,
        }),
        counts: Some(summary.counts),
        benefit: Some(summary.benefit),
        steady_benefit: Some(summary.steady_benefit),
        by_close_context: summary.by_close_context,
        by_error_class: summary.by_error_class,
        conditional_sweeps: summary.conditional_sweeps,
        join_method: Some(join_method_report()),
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

fn join_method_report() -> JoinMethodReport {
    JoinMethodReport {
        method: "wallet_id + token + reconstructed_opened_ts + future closed_ts".to_string(),
        lot_id_in_event_id: true,
        shadow_closed_trade_lot_id_available: false,
        ambiguity_note: "event_id contains shadow_lot_id, but shadow_closed_trades does not persist lot_id; report aggregates per event and surfaces multi-close matches as possible attribution ambiguity".to_string(),
    }
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

fn parse_minutes(raw: &str) -> Result<i64> {
    let value = raw
        .parse::<i64>()
        .with_context(|| format!("invalid minute value: {raw}"))?;
    if value <= 0 || value > 240 {
        anyhow::bail!("minute value must be between 1 and 240");
    }
    Ok(value)
}

fn parse_u64(raw: &str, flag: &str) -> Result<u64> {
    raw.parse::<u64>()
        .with_context(|| format!("invalid {flag}: {raw}"))
}

fn parse_sol_amount(raw: &str, flag: &str) -> Result<f64> {
    let value = raw
        .parse::<f64>()
        .with_context(|| format!("invalid {flag}: {raw}"))?;
    if !value.is_finite() || value < 0.0 || value > 1.0 {
        anyhow::bail!("{flag} must be a finite SOL amount between 0 and 1");
    }
    Ok(value)
}

fn parse_close_match_limit(raw: &str) -> Result<u32> {
    let value = raw
        .parse::<u32>()
        .with_context(|| format!("invalid --close-match-limit: {raw}"))?;
    if value == 0 || value > 100 {
        anyhow::bail!("--close-match-limit must be between 1 and 100");
    }
    Ok(value)
}

fn parse_thresholds(raw: &str) -> Result<Vec<f64>> {
    let mut thresholds = Vec::new();
    for part in raw.split(',') {
        let value = part
            .trim()
            .parse::<f64>()
            .with_context(|| format!("invalid threshold: {part}"))?;
        if !value.is_finite() || value <= 0.0 || value > 1.5 {
            anyhow::bail!("threshold must be finite and between 0 and 1.5");
        }
        thresholds.push(value);
    }
    if thresholds.is_empty() {
        anyhow::bail!("--thresholds must include at least one value");
    }
    Ok(thresholds)
}
