use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::DiscoveryService;
use copybot_storage::SqliteStore;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: clone_latest_wallet_metrics_bootstrap <db_path> --config <path> [--now <rfc3339>] [--dry-run] [--force-stale]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    run(config)
}

#[derive(Debug, Clone)]
struct Config {
    db_path: PathBuf,
    config_path: PathBuf,
    now: DateTime<Utc>,
    dry_run: bool,
    force_stale: bool,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let Some(db_path_raw) = args.next() else {
        bail!("{USAGE}");
    };
    if matches!(db_path_raw.as_str(), "--help" | "-h") {
        return Ok(None);
    };

    let mut config_path: Option<PathBuf> = None;
    let mut now: Option<DateTime<Utc>> = None;
    let mut dry_run = false;
    let mut force_stale = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--dry-run" => dry_run = true,
            "--force-stale" => force_stale = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        db_path: PathBuf::from(db_path_raw),
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        now: now.unwrap_or_else(Utc::now),
        dry_run,
        force_stale,
    }))
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn run(config: Config) -> Result<()> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let mut store = SqliteStore::open(Path::new(&config.db_path))
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let migrations_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migrations_dir).with_context(|| {
        format!(
            "failed applying migrations from {}",
            migrations_dir.display()
        )
    })?;

    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let summary = discovery.clone_latest_trusted_bootstrap_wallet_metrics(
        &store,
        config.now,
        config.dry_run,
        config.force_stale,
    )?;

    println!(
        "event=clone_latest_wallet_metrics_bootstrap source_metrics_window_start={} target_metrics_window_start={} source_snapshot_age_seconds={} source_rows={} inserted_rows={} stale_source={} forced_stale={} dry_run={} top_wallets={:?}",
        summary.source_metrics_window_start.to_rfc3339(),
        summary.target_metrics_window_start.to_rfc3339(),
        summary.source_snapshot_age_seconds,
        summary.source_rows,
        summary.inserted_rows,
        summary.stale_source,
        summary.forced_stale,
        summary.dry_run,
        summary.top_wallets,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_args_from;

    #[test]
    fn parse_args_from_returns_none_for_help_flag_before_db_path() {
        let parsed = parse_args_from(vec!["--help".to_string()]).expect("help should succeed");
        assert!(parsed.is_none());
    }

    #[test]
    fn parse_args_from_returns_none_for_help_flag_after_db_path() {
        let parsed = parse_args_from(vec![
            "bot.db".to_string(),
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--help".to_string(),
        ])
        .expect("help should succeed");
        assert!(parsed.is_none());
    }
}
