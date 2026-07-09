use crate::first_sell_full_exit_report_compute::{EntryGateAssumptions, FeeAssumptions};
use crate::first_sell_full_exit_report_db::load_audit_evidence;
use crate::first_sell_full_exit_report_summary::summarize_audit;
use crate::first_sell_full_exit_report_types::FirstSellAuditSummary;
use crate::track_b_entry_quote_report_db::open_read_only_db;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use serde::Serialize;
use std::env;
use std::path::PathBuf;

const REASON_OK: &str = "first_sell_full_exit_report_loaded";
const REASON_ERROR: &str = "first_sell_full_exit_report_error";
const DEFAULT_SINCE_HOURS: i64 = 24;
const MAX_SINCE_HOURS: i64 = 720;
const DEFAULT_ENTRY_LIMIT: u32 = 5_000;
const MAX_ENTRY_LIMIT: u32 = 10_000;
const DEFAULT_RELATED_LIMIT: u32 = 50_000;
const MAX_RELATED_LIMIT: u32 = 200_000;
const DEFAULT_BASE_FEE_LAMPORTS: u64 = 5_000;
const DEFAULT_TOKEN_ACCOUNT_RENT_LAMPORTS: u64 = 2_039_280;

#[derive(Debug, Clone, PartialEq)]
pub struct Cli {
    pub config_path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
    pub json: bool,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub outcome_until: Option<DateTime<Utc>>,
    pub since_hours: i64,
    pub entry_limit: u32,
    pub related_limit: u32,
    pub priority_fee_cap_lamports: Option<u64>,
    pub expected_entry_lamports: Option<u64>,
    pub max_entry_slippage_bps: Option<u64>,
    pub base_fee_lamports_per_leg: u64,
    pub new_ata_cash_lamports: u64,
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            config_path: None,
            db_path: None,
            json: false,
            since: None,
            until: None,
            outcome_until: None,
            since_hours: DEFAULT_SINCE_HOURS,
            entry_limit: DEFAULT_ENTRY_LIMIT,
            related_limit: DEFAULT_RELATED_LIMIT,
            priority_fee_cap_lamports: None,
            expected_entry_lamports: None,
            max_entry_slippage_bps: None,
            base_fee_lamports_per_leg: DEFAULT_BASE_FEE_LAMPORTS,
            new_ata_cash_lamports: DEFAULT_TOKEN_ACCOUNT_RENT_LAMPORTS,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct FirstSellFullExitReport {
    pub as_of: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub params: Option<ReportParams>,
    pub summary: Option<FirstSellAuditSummary>,
}

#[derive(Debug, Serialize)]
pub struct ReportParams {
    pub entry_since: DateTime<Utc>,
    pub entry_until: DateTime<Utc>,
    pub outcome_until: DateTime<Utc>,
    pub entry_limit: u32,
    pub related_limit: u32,
    pub priority_fee_cap_lamports: u64,
    pub expected_entry_lamports: u64,
    pub max_entry_slippage_bps: u64,
    pub base_fee_lamports_per_leg: u64,
    pub new_ata_cash_lamports: u64,
}

impl FirstSellFullExitReport {
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
        Ok(cli) if !cli.json => FirstSellFullExitReport::failed(as_of, "--json is required"),
        Ok(cli) => build_report(cli, as_of),
        Err(error) => FirstSellFullExitReport::failed(as_of, error.to_string()),
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("first-sell report must serialize")
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
            "--since" => cli.since = Some(parse_ts(&next_value(&mut iter, &arg)?, &arg)?),
            "--until" => cli.until = Some(parse_ts(&next_value(&mut iter, &arg)?, &arg)?),
            "--outcome-until" => {
                cli.outcome_until = Some(parse_ts(&next_value(&mut iter, &arg)?, &arg)?)
            }
            "--since-hours" => {
                cli.since_hours =
                    parse_i64(&next_value(&mut iter, &arg)?, &arg, 1, MAX_SINCE_HOURS)?
            }
            "--entry-limit" => {
                cli.entry_limit =
                    parse_u32(&next_value(&mut iter, &arg)?, &arg, 1, MAX_ENTRY_LIMIT)?
            }
            "--related-limit" => {
                cli.related_limit =
                    parse_u32(&next_value(&mut iter, &arg)?, &arg, 1, MAX_RELATED_LIMIT)?
            }
            "--priority-fee-cap-lamports" => {
                cli.priority_fee_cap_lamports =
                    Some(parse_u64(&next_value(&mut iter, &arg)?, &arg)?)
            }
            "--expected-entry-lamports" => {
                cli.expected_entry_lamports = Some(parse_u64(&next_value(&mut iter, &arg)?, &arg)?)
            }
            "--max-entry-slippage-bps" => {
                cli.max_entry_slippage_bps = Some(parse_u64(&next_value(&mut iter, &arg)?, &arg)?)
            }
            "--base-fee-lamports-per-leg" => {
                cli.base_fee_lamports_per_leg = parse_u64(&next_value(&mut iter, &arg)?, &arg)?
            }
            "--new-ata-cash-lamports" => {
                cli.new_ata_cash_lamports = parse_u64(&next_value(&mut iter, &arg)?, &arg)?
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

pub fn build_report(cli: Cli, as_of: DateTime<Utc>) -> FirstSellFullExitReport {
    match build_report_result(cli, as_of) {
        Ok(report) => report,
        Err(error) => FirstSellFullExitReport::failed(as_of, error.to_string()),
    }
}

fn build_report_result(cli: Cli, as_of: DateTime<Utc>) -> Result<FirstSellFullExitReport> {
    let context = load_context(&cli)?;
    let entry_until = cli.until.unwrap_or(as_of);
    let entry_since = cli
        .since
        .unwrap_or(entry_until - Duration::hours(cli.since_hours));
    let outcome_until = cli.outcome_until.unwrap_or(as_of);
    if entry_until <= entry_since {
        anyhow::bail!("--until must be after --since");
    }
    if outcome_until < entry_until {
        anyhow::bail!("--outcome-until must be at or after --until");
    }
    if entry_until > as_of || outcome_until > as_of {
        anyhow::bail!("--until and --outcome-until cannot be after report as_of");
    }
    let conn = open_read_only_db(&context.db_path)
        .with_context(|| format!("failed opening db {}", context.db_path.display()))?;
    let evidence = load_audit_evidence(
        &conn,
        entry_since,
        entry_until,
        outcome_until,
        cli.entry_limit,
        cli.related_limit,
    )?;
    let summary = summarize_audit(
        evidence,
        EntryGateAssumptions {
            expected_in_lamports: context.expected_entry_lamports,
            max_slippage_bps: context.max_entry_slippage_bps,
        },
        FeeAssumptions {
            priority_fee_cap_lamports: context.priority_fee_cap_lamports,
            base_fee_lamports_per_leg: cli.base_fee_lamports_per_leg,
            new_ata_cash_lamports: cli.new_ata_cash_lamports,
        },
        outcome_until,
    );
    Ok(FirstSellFullExitReport {
        as_of,
        reason_class: REASON_OK.to_string(),
        error: None,
        params: Some(ReportParams {
            entry_since,
            entry_until,
            outcome_until,
            entry_limit: cli.entry_limit,
            related_limit: cli.related_limit,
            priority_fee_cap_lamports: context.priority_fee_cap_lamports,
            expected_entry_lamports: context.expected_entry_lamports,
            max_entry_slippage_bps: context.max_entry_slippage_bps,
            base_fee_lamports_per_leg: cli.base_fee_lamports_per_leg,
            new_ata_cash_lamports: cli.new_ata_cash_lamports,
        }),
        summary: Some(summary),
    })
}

struct LoadedContext {
    db_path: PathBuf,
    priority_fee_cap_lamports: u64,
    expected_entry_lamports: u64,
    max_entry_slippage_bps: u64,
}

fn load_context(cli: &Cli) -> Result<LoadedContext> {
    let config = cli
        .config_path
        .as_ref()
        .map(|path| {
            load_from_path(path)
                .with_context(|| format!("failed loading config {}", path.display()))
        })
        .transpose()?;
    let db_path = cli
        .db_path
        .clone()
        .or_else(|| {
            config
                .as_ref()
                .map(|config| PathBuf::from(config.sqlite.path.clone()))
        })
        .ok_or_else(|| anyhow!("either --config or --db-path is required"))?;
    let config_cap = config
        .as_ref()
        .map(|config| config.execution.pretrade_max_priority_fee_lamports);
    let config_entry_lamports = config
        .as_ref()
        .map(|config| sol_to_lamports(config.execution.canary_buy_size_sol))
        .transpose()?;
    let config_slippage = config.as_ref().map(|config| {
        let directional = config.execution.quote_canary_buy_slippage_bps;
        if directional > 0 {
            directional
        } else {
            config.execution.quote_canary_slippage_bps
        }
    });
    let expected_entry_lamports = cli
        .expected_entry_lamports
        .or(config_entry_lamports)
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("--expected-entry-lamports must be positive with --db-path"))?;
    Ok(LoadedContext {
        db_path,
        priority_fee_cap_lamports: cli.priority_fee_cap_lamports.or(config_cap).unwrap_or(0),
        expected_entry_lamports,
        max_entry_slippage_bps: cli
            .max_entry_slippage_bps
            .or(config_slippage)
            .ok_or_else(|| anyhow!("--max-entry-slippage-bps is required with --db-path"))?,
    })
}

fn sol_to_lamports(sol: f64) -> Result<u64> {
    let lamports = (sol * 1_000_000_000.0).round();
    if !sol.is_finite()
        || sol <= 0.0
        || !lamports.is_finite()
        || lamports <= 0.0
        || lamports > u64::MAX as f64
    {
        anyhow::bail!("invalid execution.canary_buy_size_sol: {sol}");
    }
    Ok(lamports as u64)
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

fn parse_i64(raw: &str, flag: &str, min: i64, max: i64) -> Result<i64> {
    let value = raw
        .parse::<i64>()
        .with_context(|| format!("invalid {flag}: {raw}"))?;
    if !(min..=max).contains(&value) {
        anyhow::bail!("{flag} must be between {min} and {max}");
    }
    Ok(value)
}

fn parse_u32(raw: &str, flag: &str, min: u32, max: u32) -> Result<u32> {
    let value = raw
        .parse::<u32>()
        .with_context(|| format!("invalid {flag}: {raw}"))?;
    if !(min..=max).contains(&value) {
        anyhow::bail!("{flag} must be between {min} and {max}");
    }
    Ok(value)
}

fn parse_u64(raw: &str, flag: &str) -> Result<u64> {
    raw.parse::<u64>()
        .with_context(|| format!("invalid {flag}: {raw}"))
}
