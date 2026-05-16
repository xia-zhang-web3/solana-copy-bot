use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use copybot_config::load_from_path;
use copybot_discovery_v2::{
    build_discovery_v2_status, live_portfolio_rpc_url_from_config,
    load_materialized_discovery_v2_status_for_publish, publish_discovery_v2_status,
    DiscoveryV2BuildOptions, DiscoveryV2PublishReport,
};
use copybot_storage_core::{
    ensure_discovery_v2_schema, validate_discovery_v2_status_schema_read_only, SqliteDiscoveryStore,
};
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_v2_publish --config <path> [--db-path <path>] [--materialized-status] [--commit --acknowledge-daemon-restart-required]";

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
    commit: bool,
    acknowledge_daemon_restart_required: bool,
    materialized_status: bool,
}

fn parse_args() -> Result<Option<Config>> {
    let mut args = env::args().skip(1);
    let mut config_path = None;
    let mut db_path = None;
    let mut commit = false;
    let mut acknowledge_daemon_restart_required = false;
    let mut materialized_status = false;
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
                "{arg} is not accepted by production publish; use config values and wall clock"
            ),
            "--commit" => commit = true,
            "--acknowledge-daemon-restart-required" => acknowledge_daemon_restart_required = true,
            "--materialized-status" => materialized_status = true,
            "--dry-run" => commit = false,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
        commit,
        acknowledge_daemon_restart_required,
        materialized_status,
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
    let read_options = DiscoveryV2BuildOptions::from_config(
        &loaded.discovery,
        loaded.execution.enabled,
        Utc::now(),
    )
    .with_live_portfolio_rpc_url(live_portfolio_rpc_url_from_config(&loaded));
    let read_store = SqliteDiscoveryStore::open_read_only(&db_path)
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    read_store
        .tune_for_operator_scans()
        .context("failed tuning sqlite connection for discovery v2 publish")?;
    validate_discovery_v2_status_schema_read_only(&read_store).with_context(|| {
        format!(
            "sqlite db is not ready for discovery v2 publish: {}; run schema preparation before --commit",
            db_path.display()
        )
    })?;
    let status = if config.materialized_status {
        load_materialized_discovery_v2_status_for_publish(
            &read_store,
            &loaded.discovery,
            &loaded.shadow,
            &read_options,
        )?
        .0
    } else {
        build_discovery_v2_status(
            &read_store,
            &loaded.discovery,
            &loaded.shadow,
            read_options.clone(),
        )?
    };
    if !config.commit {
        return publish_discovery_v2_status(&read_store, status, false);
    }
    if !config.acknowledge_daemon_restart_required {
        bail!(
            "--commit requires --acknowledge-daemon-restart-required because copybot-app samples publication truth at startup"
        );
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
    publish_discovery_v2_status(&write_store, status, true)
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
