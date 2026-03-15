use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::DiscoveryService;
use copybot_storage::SqliteStore;
use std::env;
use std::path::{Path, PathBuf};

fn main() -> Result<()> {
    let config = parse_args()?;
    run(config)
}

#[derive(Debug, Clone)]
struct Config {
    db_path: PathBuf,
    config_path: PathBuf,
    now: DateTime<Utc>,
    allow_rpc: bool,
}

fn parse_args() -> Result<Config> {
    let mut args = env::args().skip(1);
    let Some(db_path_raw) = args.next() else {
        bail!(
            "usage: materialize_wallet_metrics_bootstrap <db_path> --config <path> [--now <rfc3339>] [--allow-rpc]"
        );
    };

    let mut config_path: Option<PathBuf> = None;
    let mut now: Option<DateTime<Utc>> = None;
    let mut allow_rpc = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--allow-rpc" => allow_rpc = true,
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Config {
        db_path: PathBuf::from(db_path_raw),
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        now: now.unwrap_or_else(Utc::now),
        allow_rpc,
    })
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

fn select_role_helius_http_url(role_http_url: &str, fallback_http_url: &str) -> Option<String> {
    [role_http_url, fallback_http_url]
        .into_iter()
        .map(str::trim)
        .find(|url| !url.is_empty())
        .map(ToString::to_string)
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

    let helius_http_url = if config.allow_rpc {
        select_role_helius_http_url(
            &loaded_config.discovery.helius_http_url,
            &loaded_config.ingestion.helius_http_url,
        )
    } else {
        None
    };
    let discovery = DiscoveryService::new_with_helius(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
        helius_http_url,
    );
    let summary = discovery.materialize_trusted_bootstrap_wallet_metrics(&store, config.now)?;

    println!(
        "event=materialize_wallet_metrics_bootstrap metrics_window_start={} observed_swaps_loaded={} wallets_seen={} eligible_wallets={} metrics_written={} bucket_already_exists={} rpc_enabled={} top_wallets={:?}",
        summary.metrics_window_start.to_rfc3339(),
        summary.observed_swaps_loaded,
        summary.wallets_seen,
        summary.eligible_wallets,
        summary.metrics_written,
        summary.bucket_already_exists,
        config.allow_rpc,
        summary.top_wallets,
    );

    Ok(())
}
