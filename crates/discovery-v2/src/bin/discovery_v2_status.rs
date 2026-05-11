use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use copybot_config::load_from_path;
use copybot_discovery_v2::{build_discovery_v2_status, DiscoveryV2BuildOptions, DiscoveryV2Status};
use copybot_storage_core::{validate_discovery_v2_status_schema_read_only, SqliteDiscoveryStore};
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_v2_status --config <path> [--db-path <path>]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let status = run(config)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&status.bounded_operator_wallet_metrics())?
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    db_path: Option<PathBuf>,
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
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--window-minutes"
            | "--max-tail-lag-seconds"
            | "--max-rows"
            | "--time-budget-ms"
            | "--now" => bail!(
                "{arg} is not accepted by production status; use config values and wall clock"
            ),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
    }))
}

fn run(config: Config) -> Result<DiscoveryV2Status> {
    let loaded = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded.sqlite.path,
    );
    let store = SqliteDiscoveryStore::open_read_only(&db_path)
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    validate_discovery_v2_status_schema_read_only(&store).with_context(|| {
        format!(
            "sqlite db is not discovery v2 schema-ready: {}",
            db_path.display()
        )
    })?;
    let options = DiscoveryV2BuildOptions::from_config(
        &loaded.discovery,
        loaded.execution.enabled,
        Utc::now(),
    );
    build_discovery_v2_status(&store, &loaded.discovery, &loaded.shadow, options)
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
