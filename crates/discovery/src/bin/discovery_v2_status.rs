use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::discovery_v2::{
    build_discovery_v2_status, DiscoveryV2BuildOptions, DiscoveryV2Status,
};
use copybot_storage::SqliteStore;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_v2_status --config <path> [--db-path <path>] [--window-minutes <n>] [--max-tail-lag-seconds <n>] [--max-rows <n>] [--time-budget-ms <n>] [--now <rfc3339>]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let status = run(config)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&status).context("failed serializing discovery v2 status")?
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    db_path: Option<PathBuf>,
    window_minutes: Option<u64>,
    max_tail_lag_seconds: Option<u64>,
    max_rows: Option<usize>,
    time_budget_ms: Option<u64>,
    now: DateTime<Utc>,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path = None;
    let mut db_path = None;
    let mut window_minutes = None;
    let mut max_tail_lag_seconds = None;
    let mut max_rows = None;
    let mut time_budget_ms = None;
    let mut now = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?));
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?));
            }
            "--window-minutes" => {
                window_minutes = Some(parse_u64_arg("--window-minutes", args.next())?);
            }
            "--max-tail-lag-seconds" => {
                max_tail_lag_seconds = Some(parse_u64_arg("--max-tail-lag-seconds", args.next())?);
            }
            "--max-rows" => {
                max_rows = Some(parse_usize_arg("--max-rows", args.next())?);
            }
            "--time-budget-ms" => {
                time_budget_ms = Some(parse_u64_arg("--time-budget-ms", args.next())?);
            }
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
        window_minutes,
        max_tail_lag_seconds,
        max_rows,
        time_budget_ms,
        now: now.unwrap_or_else(Utc::now),
    }))
}

fn run(config: Config) -> Result<DiscoveryV2Status> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let store = SqliteStore::open_read_only(&db_path)
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    let mut options = DiscoveryV2BuildOptions::from_config(
        &loaded_config.discovery,
        loaded_config.execution.enabled,
        config.now,
    );
    if let Some(window_minutes) = config.window_minutes {
        options.window_minutes = positive_u64("--window-minutes", window_minutes)?;
    }
    if let Some(max_tail_lag_seconds) = config.max_tail_lag_seconds {
        options.max_tail_lag_seconds =
            positive_u64("--max-tail-lag-seconds", max_tail_lag_seconds)?;
    }
    if let Some(max_rows) = config.max_rows {
        options.max_rows = positive_usize("--max-rows", max_rows)?;
    }
    if let Some(time_budget_ms) = config.time_budget_ms {
        options.time_budget_ms = positive_u64("--time-budget-ms", time_budget_ms)?;
    }
    build_discovery_v2_status(
        &store,
        &loaded_config.discovery,
        &loaded_config.shadow,
        options,
    )
}

fn resolve_db_path(
    config_path: &Path,
    db_path_override: Option<&Path>,
    configured_db_path: &str,
) -> PathBuf {
    if let Some(db_path_override) = db_path_override {
        return db_path_override.to_path_buf();
    }
    let configured_db_path = PathBuf::from(configured_db_path);
    if configured_db_path.is_absolute() {
        return configured_db_path;
    }
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(configured_db_path)
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid {flag} integer: {raw}"))
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<usize>()
        .with_context(|| format!("invalid {flag} integer: {raw}"))
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn positive_u64(flag: &str, value: u64) -> Result<u64> {
    if value == 0 {
        bail!("{flag} must be greater than zero");
    }
    Ok(value)
}

fn positive_usize(flag: &str, value: usize) -> Result<usize> {
    if value == 0 {
        bail!("{flag} must be greater than zero");
    }
    Ok(value)
}
