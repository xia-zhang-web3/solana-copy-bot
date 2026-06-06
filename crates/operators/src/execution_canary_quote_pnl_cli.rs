use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use std::path::PathBuf;

use super::{Cli, MAX_LIMIT, MAX_SINCE_HOURS};

pub(super) fn resolve_db_path(cli: &Cli) -> Result<PathBuf> {
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

pub(super) fn next_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

pub(super) fn parse_limit(raw: &str) -> Result<u32> {
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

pub(super) fn parse_since(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("invalid --since rfc3339 value: {raw}"))
        .map(|value| value.with_timezone(&Utc))
}

pub(super) fn parse_since_hours(raw: &str) -> Result<i64> {
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
