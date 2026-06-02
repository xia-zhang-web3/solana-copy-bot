use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_storage_core::{
    ExecutionCanaryReadinessSummary, ExecutionCanaryReadinessWindowSummary, SqliteStore,
};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

const REASON_OK: &str = "execution_canary_readiness_loaded";
const REASON_CLI_ERROR: &str = "execution_canary_readiness_cli_error";
const REASON_CONFIG_UNREADABLE: &str = "execution_canary_readiness_config_unreadable";
const REASON_DB_UNREADABLE: &str = "execution_canary_readiness_db_unreadable";
const REASON_JSON_REQUIRED: &str = "execution_canary_readiness_json_required";
const DEFAULT_WINDOW_LIMIT: u32 = 50;
const MAX_WINDOW_LIMIT: u32 = 200;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    pub config_path: Option<PathBuf>,
    pub db_path: Option<PathBuf>,
    pub json: bool,
    pub limit: u32,
}

#[derive(Debug, Serialize)]
pub struct CanaryReadinessOperatorReport {
    pub config_loaded: bool,
    pub db_opened: bool,
    pub as_of: DateTime<Utc>,
    pub reason_class: String,
    pub error: Option<String>,
    pub readiness_green: bool,
    pub production_green: bool,
    pub summary: Option<ExecutionCanaryReadinessSummary>,
    pub window: Option<ExecutionCanaryReadinessWindowSummary>,
}

impl CanaryReadinessOperatorReport {
    fn failed(reason_class: &str, error: Option<String>, as_of: DateTime<Utc>) -> Self {
        Self {
            config_loaded: false,
            db_opened: false,
            as_of,
            reason_class: reason_class.to_string(),
            error,
            readiness_green: false,
            production_green: false,
            summary: None,
            window: None,
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
        Ok(cli) if !cli.json => CanaryReadinessOperatorReport::failed(
            REASON_JSON_REQUIRED,
            Some("--json is required for operator output".to_string()),
            as_of,
        ),
        Ok(cli) => build_report(cli, as_of),
        Err(error) => {
            CanaryReadinessOperatorReport::failed(REASON_CLI_ERROR, Some(error.to_string()), as_of)
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
    let mut limit = DEFAULT_WINDOW_LIMIT;
    let mut iter = args.into_iter().map(Into::into);

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(next_value(&mut iter, "--config")?));
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(next_value(&mut iter, "--db-path")?));
            }
            "--limit" => {
                limit = parse_limit(&next_value(&mut iter, "--limit")?)?;
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
    })
}

pub fn build_report_from_db_path(
    db_path: &Path,
    as_of: DateTime<Utc>,
) -> CanaryReadinessOperatorReport {
    build_report(
        Cli {
            config_path: None,
            db_path: Some(db_path.to_path_buf()),
            json: true,
            limit: DEFAULT_WINDOW_LIMIT,
        },
        as_of,
    )
}

fn build_report(cli: Cli, as_of: DateTime<Utc>) -> CanaryReadinessOperatorReport {
    let db_path = match resolve_db_path(&cli) {
        Ok(path) => path,
        Err(error) => {
            return CanaryReadinessOperatorReport::failed(
                REASON_CONFIG_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };

    let store = match SqliteStore::open_read_only(&db_path) {
        Ok(store) => store,
        Err(error) => {
            return CanaryReadinessOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    let summary = match store.execution_canary_readiness_summary(as_of) {
        Ok(summary) => summary,
        Err(error) => {
            return CanaryReadinessOperatorReport::failed(
                REASON_DB_UNREADABLE,
                Some(error.to_string()),
                as_of,
            );
        }
    };
    match store.execution_canary_readiness_window_summary(as_of, cli.limit) {
        Ok(window) => CanaryReadinessOperatorReport {
            config_loaded: cli.config_path.is_some(),
            db_opened: true,
            as_of,
            readiness_green: summary.readiness_status == "would_enter",
            production_green: false,
            reason_class: REASON_OK.to_string(),
            error: None,
            summary: Some(summary),
            window: Some(window),
        },
        Err(error) => CanaryReadinessOperatorReport::failed(
            REASON_DB_UNREADABLE,
            Some(error.to_string()),
            as_of,
        ),
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
    if limit == 0 || limit > MAX_WINDOW_LIMIT {
        return Err(anyhow!(
            "--limit must be between 1 and {MAX_WINDOW_LIMIT}, got {limit}"
        ));
    }
    Ok(limit)
}
