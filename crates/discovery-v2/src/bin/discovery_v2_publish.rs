use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery_v2::{
    build_discovery_v2_status, publish_discovery_v2_status, DiscoveryV2BuildOptions,
    DiscoveryV2PublishReport,
};
use copybot_storage_core::{
    ensure_discovery_v2_schema, validate_discovery_v2_schema_read_only, SqliteDiscoveryStore,
};
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_v2_publish --config <path> [--db-path <path>] [--window-minutes <n>] [--max-tail-lag-seconds <n>] [--max-rows <n>] [--time-budget-ms <n>] [--now <rfc3339>] [--commit]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let report = run(config)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
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
    commit: bool,
}

fn parse_args() -> Result<Option<Config>> {
    let mut args = env::args().skip(1);
    let mut config_path = None;
    let mut db_path = None;
    let mut window_minutes = None;
    let mut max_tail_lag_seconds = None;
    let mut max_rows = None;
    let mut time_budget_ms = None;
    let mut now = None;
    let mut commit = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--window-minutes" => {
                window_minutes = Some(parse_u64_arg("--window-minutes", args.next())?)
            }
            "--max-tail-lag-seconds" => {
                max_tail_lag_seconds = Some(parse_u64_arg("--max-tail-lag-seconds", args.next())?)
            }
            "--max-rows" => max_rows = Some(parse_usize_arg("--max-rows", args.next())?),
            "--time-budget-ms" => {
                time_budget_ms = Some(parse_u64_arg("--time-budget-ms", args.next())?)
            }
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--commit" => commit = true,
            "--dry-run" => commit = false,
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
        commit,
    }))
}

fn run(config: Config) -> Result<DiscoveryV2PublishReport> {
    let loaded = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded.sqlite.path,
    );
    let mut options = DiscoveryV2BuildOptions::from_config(
        &loaded.discovery,
        loaded.execution.enabled,
        config.now,
    );
    apply_overrides(&mut options, &config)?;
    let read_store = SqliteDiscoveryStore::open_read_only(&db_path)
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    validate_discovery_v2_schema_read_only(&read_store).with_context(|| {
        format!(
            "sqlite db is not ready for discovery v2 publish: {}; run schema preparation before --commit",
            db_path.display()
        )
    })?;
    let status = build_discovery_v2_status(
        &read_store,
        &loaded.discovery,
        &loaded.shadow,
        options.clone(),
    )?;
    if !config.commit {
        return publish_discovery_v2_status(&read_store, status, false);
    }
    if !status.production_green {
        return publish_discovery_v2_status(&read_store, status, true);
    }
    drop(read_store);

    let write_store = SqliteDiscoveryStore::open(&db_path)
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    ensure_discovery_v2_schema(&write_store).with_context(|| {
        format!(
            "failed ensuring discovery v2 schema for {}",
            db_path.display()
        )
    })?;
    let status =
        build_discovery_v2_status(&write_store, &loaded.discovery, &loaded.shadow, options)?;
    publish_discovery_v2_status(&write_store, status, true)
}

fn apply_overrides(options: &mut DiscoveryV2BuildOptions, config: &Config) -> Result<()> {
    if let Some(value) = config.window_minutes {
        options.window_minutes = positive_u64("--window-minutes", value)?;
    }
    if let Some(value) = config.max_tail_lag_seconds {
        options.max_tail_lag_seconds = positive_u64("--max-tail-lag-seconds", value)?;
    }
    if let Some(value) = config.max_rows {
        options.max_rows = positive_usize("--max-rows", value)?;
    }
    if let Some(value) = config.time_budget_ms {
        options.time_budget_ms = positive_u64("--time-budget-ms", value)?;
    }
    Ok(())
}

fn resolve_db_path(config_path: &Path, override_path: Option<&Path>, configured: &str) -> PathBuf {
    if let Some(override_path) = override_path {
        return override_path.to_path_buf();
    }
    let configured = PathBuf::from(configured);
    if configured.is_absolute() {
        configured
    } else {
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(configured)
    }
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
    parse_string_arg(flag, value)?
        .parse::<u64>()
        .context("invalid integer")
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    parse_string_arg(flag, value)?
        .parse::<usize>()
        .context("invalid integer")
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
