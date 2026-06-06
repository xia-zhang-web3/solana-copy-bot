use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_storage_core::{
    ExecutionCanaryQuotePnlSummary, ExecutionQuoteCanaryProviderComparisonSummary, SqliteStore,
};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

use crate::execution_canary_quote_pnl_gate::build_tiny_execution_gate;
pub use crate::execution_canary_quote_pnl_gate::{TinyExecutionGate, TinyExecutionGateCheck};

const REASON_OK: &str = "execution_canary_quote_pnl_loaded";
const REASON_CLI_ERROR: &str = "execution_canary_quote_pnl_cli_error";
const REASON_CONFIG_UNREADABLE: &str = "execution_canary_quote_pnl_config_unreadable";
const REASON_DB_UNREADABLE: &str = "execution_canary_quote_pnl_db_unreadable";
const REASON_JSON_REQUIRED: &str = "execution_canary_quote_pnl_json_required";
const DEFAULT_LIMIT: u32 = 50;
const MAX_LIMIT: u32 = 200;
const DEFAULT_SINCE_HOURS: i64 = 24;
const MAX_SINCE_HOURS: i64 = 168;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    pub config_path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
    pub json: bool,
    pub limit: u32,
    pub since: Option<DateTime<Utc>>,
    pub since_hours: i64,
}

#[derive(Debug, Serialize)]
pub struct CanaryQuotePnlOperatorReport {
    pub config_loaded: bool,
    pub db_opened: bool,
    pub as_of: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub summary: Option<ExecutionCanaryQuotePnlSummary>,
    pub provider_comparison: Option<ExecutionQuoteCanaryProviderComparisonSummary>,
    pub tiny_execution_gate: Option<TinyExecutionGate>,
}

impl CanaryQuotePnlOperatorReport {
    fn failed(reason_class: &str, error: Option<String>, as_of: DateTime<Utc>) -> Self {
        Self {
            config_loaded: false,
            db_opened: false,
            as_of,
            since: as_of - Duration::hours(DEFAULT_SINCE_HOURS),
            reason_class: reason_class.to_string(),
            error,
            summary: None,
            provider_comparison: None,
            tiny_execution_gate: None,
        }
    }

    fn exit_code(&self) -> i32 {
        match self.reason_class.as_str() {
            REASON_OK => 0,
            _ => 1,
        }
    }
}

pub fn run_from_env() -> i32 {
    let as_of = Utc::now();
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(cli) if !cli.json => CanaryQuotePnlOperatorReport::failed(
            REASON_JSON_REQUIRED,
            Some("--json is required for operator output".to_string()),
            as_of,
        ),
        Ok(cli) => build_report(cli, as_of),
        Err(error) => {
            CanaryQuotePnlOperatorReport::failed(REASON_CLI_ERROR, Some(error.to_string()), as_of)
        }
    };

    println!(
        "{}",
        serde_json::to_string(&report).expect("operator report must serialize")
    );
    report.exit_code()
}

pub fn parse_args_from<I>(args: I) -> Result<Cli>
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let mut config_path = None;
    let mut db_path = None;
    let mut json = false;
    let mut limit = DEFAULT_LIMIT;
    let mut since = None;
    let mut since_hours = DEFAULT_SINCE_HOURS;
    let mut iter = args.into_iter().map(Into::into);

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => config_path = Some(PathBuf::from(next_value(&mut iter, "--config")?)),
            "--db-path" => db_path = Some(PathBuf::from(next_value(&mut iter, "--db-path")?)),
            "--limit" => limit = parse_limit(&next_value(&mut iter, "--limit")?)?,
            "--since" => since = Some(parse_since(&next_value(&mut iter, "--since")?)?),
            "--since-hours" => {
                since_hours = parse_since_hours(&next_value(&mut iter, "--since-hours")?)?;
            }
            "--json" => json = true,
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }

    if config_path.is_none() && db_path.is_none() {
        return Err(anyhow!("either --config or --db-path is required"));
    }

    Ok(Cli {
        config_path,
        db_path,
        json,
        limit,
        since,
        since_hours,
    })
}

pub fn build_report_from_db_path(
    db_path: &Path,
    as_of: DateTime<Utc>,
) -> CanaryQuotePnlOperatorReport {
    build_report(
        Cli {
            config_path: None,
            db_path: Some(db_path.to_path_buf()),
            json: true,
            limit: DEFAULT_LIMIT,
            since: None,
            since_hours: DEFAULT_SINCE_HOURS,
        },
        as_of,
    )
}

fn build_report(cli: Cli, as_of: DateTime<Utc>) -> CanaryQuotePnlOperatorReport {
    let since = cli
        .since
        .unwrap_or(as_of - Duration::hours(cli.since_hours));
    let db_path = match resolve_db_path(&cli) {
        Ok(path) => path,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_CONFIG_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };

    let store = match SqliteStore::open_read_only(&db_path) {
        Ok(store) => store,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    let summary = match store.execution_canary_quote_pnl_summary(as_of, since, cli.limit) {
        Ok(summary) => summary,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    let canary_status = match store.execution_canary_status_report(as_of) {
        Ok(report) => report,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    let canary_readiness = match store.execution_canary_readiness_summary(as_of) {
        Ok(report) => report,
        Err(error) => {
            return CanaryQuotePnlOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    let provider_comparison =
        match store.execution_quote_canary_provider_comparison_summary(as_of, since, cli.limit) {
            Ok(summary) => summary,
            Err(error) => {
                return CanaryQuotePnlOperatorReport::failed(
                    REASON_DB_UNREADABLE,
                    Some(error.to_string()),
                    as_of,
                );
            }
        };
    let recent_loss =
        match store.execution_canary_realized_loss_sol_since(as_of - Duration::hours(24)) {
            Ok(loss) => loss,
            Err(error) => {
                return CanaryQuotePnlOperatorReport::failed(
                    REASON_DB_UNREADABLE,
                    Some(error.to_string()),
                    as_of,
                );
            }
        };
    let tiny_execution_gate = build_tiny_execution_gate(
        &summary,
        &canary_status,
        &canary_readiness,
        recent_loss,
        as_of,
    );

    CanaryQuotePnlOperatorReport {
        config_loaded: cli.config_path.is_some(),
        db_opened: true,
        as_of,
        since,
        reason_class: REASON_OK.to_string(),
        error: None,
        summary: Some(summary),
        provider_comparison: Some(provider_comparison),
        tiny_execution_gate: Some(tiny_execution_gate),
    }
}

fn resolve_db_path(cli: &Cli) -> Result<PathBuf> {
    if let Some(path) = &cli.db_path {
        return Ok(path.clone());
    }
    let config_path = cli
        .config_path
        .as_ref()
        .ok_or_else(|| anyhow!("--config is required when --db-path is omitted"))?;
    let loaded = load_from_path(config_path)
        .with_context(|| format!("failed to load config: {}", config_path.display()))?;
    Ok(PathBuf::from(loaded.sqlite.path))
}

fn next_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn parse_limit(raw: &str) -> Result<u32> {
    let limit = raw
        .parse::<u32>()
        .with_context(|| format!("invalid --limit value: {raw}"))?;
    if limit == 0 || limit > MAX_LIMIT {
        return Err(anyhow!(
            "--limit must be between 1 and {MAX_LIMIT}, got {limit}"
        ));
    }
    Ok(limit)
}

fn parse_since(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("invalid --since rfc3339 value: {raw}"))
        .map(|value| value.with_timezone(&Utc))
}

fn parse_since_hours(raw: &str) -> Result<i64> {
    let hours = raw
        .parse::<i64>()
        .with_context(|| format!("invalid --since-hours value: {raw}"))?;
    if hours <= 0 || hours > MAX_SINCE_HOURS {
        return Err(anyhow!(
            "--since-hours must be between 1 and {MAX_SINCE_HOURS}, got {hours}"
        ));
    }
    Ok(hours)
}
