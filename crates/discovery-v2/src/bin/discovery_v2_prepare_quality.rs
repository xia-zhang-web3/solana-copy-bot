use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use copybot_config::{load_from_path, AppConfig};
use copybot_discovery_v2::{
    live_portfolio_rpc_url_from_config, materialize_discovery_v2_status,
    prepare_discovery_v2_quality, reusable_materialized_discovery_v2_status_for_prepare,
    DiscoveryV2BuildOptions, DiscoveryV2MaterializedStatusReport, DiscoveryV2PrepareQualityMode,
    DiscoveryV2PrepareQualityOptions, DiscoveryV2PrepareQualityReport,
};
use copybot_storage_core::{
    ensure_discovery_v2_schema, validate_discovery_v2_status_schema_read_only, SqliteDiscoveryStore,
};
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_v2_prepare_quality --config <path> [--db-path <path>] [--max-mints <n>] [--commit] [--incremental] [--materialize-status]";

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
    max_mints: usize,
    commit: bool,
    incremental: bool,
    materialize_status: bool,
}

#[derive(Debug, serde::Serialize)]
struct PrepareQualityCliReport {
    #[serde(flatten)]
    quality: DiscoveryV2PrepareQualityReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    materialized_status: Option<DiscoveryV2MaterializedStatusReport>,
}

fn parse_args() -> Result<Option<Config>> {
    let mut args = env::args().skip(1);
    let mut config_path = None;
    let mut db_path = None;
    let mut max_mints = 10_000usize;
    let mut commit = false;
    let mut incremental = false;
    let mut materialize_status = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--max-mints" => max_mints = parse_usize_arg("--max-mints", args.next())?,
            "--commit" => commit = true,
            "--dry-run" => commit = false,
            "--incremental" => incremental = true,
            "--materialize-status" => materialize_status = true,
            "--window-minutes"
            | "--max-tail-lag-seconds"
            | "--max-rows"
            | "--time-budget-ms"
            | "--now" => bail!(
                "{arg} is not accepted by production quality prepare; use config values and wall clock"
            ),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }
    if materialize_status && !commit {
        bail!("--materialize-status requires --commit");
    }
    if incremental && !commit {
        bail!("--incremental requires --commit; use a copied DB for dry-run comparison");
    }
    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
        max_mints,
        commit,
        incremental,
        materialize_status,
    }))
}

fn run(config: Config) -> Result<PrepareQualityCliReport> {
    let loaded = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded.sqlite.path,
    );
    let options = DiscoveryV2PrepareQualityOptions::from_config(
        &loaded.discovery,
        Utc::now(),
        config.max_mints,
        config.commit,
    )
    .with_mode(if config.incremental {
        DiscoveryV2PrepareQualityMode::Incremental
    } else {
        DiscoveryV2PrepareQualityMode::FullScan
    });
    if config.commit {
        let store = SqliteDiscoveryStore::open(&db_path)
            .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
        ensure_discovery_v2_schema(&store).context("failed ensuring discovery v2 schema")?;
        if !config.incremental {
            if config.materialize_status {
                let build_options = materialized_status_build_options(&loaded);
                if let Some(report) = reusable_materialized_discovery_v2_status_for_prepare(
                    &store,
                    &loaded.discovery,
                    &loaded.shadow,
                    &build_options,
                )? {
                    let quality =
                        DiscoveryV2PrepareQualityReport::skipped_for_reusable_materialized_status(
                            &options,
                        );
                    return Ok(PrepareQualityCliReport {
                        quality,
                        materialized_status: Some(report),
                    });
                }
            }
        }
        let quality =
            prepare_discovery_v2_quality(&store, &loaded.discovery, &loaded.shadow, options)?;
        let materialized_status = if config.materialize_status && quality.committed {
            let build_options = materialized_status_build_options(&loaded);
            if let Some(report) = reusable_materialized_discovery_v2_status_for_prepare(
                &store,
                &loaded.discovery,
                &loaded.shadow,
                &build_options,
            )? {
                Some(report)
            } else {
                let (_status, report) = materialize_discovery_v2_status(
                    &store,
                    &loaded.discovery,
                    &loaded.shadow,
                    build_options,
                )?;
                Some(report)
            }
        } else {
            None
        };
        return Ok(PrepareQualityCliReport {
            quality,
            materialized_status,
        });
    }
    let store = SqliteDiscoveryStore::open_read_only(&db_path)
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    validate_discovery_v2_status_schema_read_only(&store)
        .context("sqlite db is not discovery v2 schema-ready")?;
    let quality = prepare_discovery_v2_quality(&store, &loaded.discovery, &loaded.shadow, options)?;
    Ok(PrepareQualityCliReport {
        quality,
        materialized_status: None,
    })
}

fn materialized_status_build_options(loaded: &AppConfig) -> DiscoveryV2BuildOptions {
    DiscoveryV2BuildOptions::from_config(&loaded.discovery, loaded.execution.enabled, Utc::now())
        .with_live_portfolio_rpc_url(live_portfolio_rpc_url_from_config(loaded))
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

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    let parsed = raw
        .parse::<usize>()
        .with_context(|| format!("invalid numeric value for {flag}: {raw}"))?;
    if parsed == 0 {
        bail!("{flag} must be greater than zero");
    }
    Ok(parsed)
}
