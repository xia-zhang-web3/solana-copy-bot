#[path = "../wallet_report_cli.rs"]
mod wallet_report_cli;

use anyhow::{Context, Result};
use chrono::Utc;
use copybot_config::load_from_path;
use copybot_discovery_v2::{
    build_discovery_v2_rug_feedback_distribution_report, build_discovery_v2_status,
    build_discovery_v2_wallet_report, live_portfolio_rpc_url_from_config,
    load_discovery_v2_shadow_signal_status, load_materialized_discovery_v2_status_for_publish,
    DiscoveryV2BuildOptions, DiscoveryV2RugFeedbackDistributionOptions, DiscoveryV2Status,
    DiscoveryV2WalletReportOptions,
};
use copybot_storage_core::{validate_discovery_v2_status_schema_read_only, SqliteDiscoveryStore};
use std::path::{Path, PathBuf};
use wallet_report_cli::{parse_args, WalletReportCliConfig, USAGE};

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let report = run(config)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn run(config: WalletReportCliConfig) -> Result<serde_json::Value> {
    let mut loaded = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    if config.simulate_rug_filter {
        loaded.discovery.rug_wallet_filter_enabled = true;
        if let Some(rate) = config.rug_filter_max_stale_terminal_rate {
            loaded.discovery.rug_wallet_filter_max_stale_terminal_rate = rate;
        }
        if let Some(pnl) = config.rug_filter_max_stale_terminal_pnl_sol {
            loaded
                .discovery
                .rug_wallet_filter_max_stale_terminal_pnl_sol = pnl;
        }
    }
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded.sqlite.path,
    );
    let store = SqliteDiscoveryStore::open_read_only(&db_path)
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    store
        .tune_for_operator_scans()
        .context("failed tuning sqlite connection for discovery v2 wallet report")?;
    validate_discovery_v2_status_schema_read_only(&store).with_context(|| {
        format!(
            "sqlite db is not discovery v2 schema-ready: {}",
            db_path.display()
        )
    })?;
    let now = Utc::now();
    if let Some(since) = config.rug_feedback_since {
        let report = build_discovery_v2_rug_feedback_distribution_report(
            &store,
            DiscoveryV2RugFeedbackDistributionOptions {
                generated_at: now,
                since,
                until: config.rug_feedback_until.unwrap_or(now),
                min_closed_trades: config
                    .rug_feedback_min_closed_trades
                    .unwrap_or(loaded.discovery.rug_wallet_filter_min_closed_trades),
                max_stale_terminal_rate: config
                    .rug_feedback_rate_threshold
                    .unwrap_or(loaded.discovery.rug_wallet_filter_max_stale_terminal_rate),
                max_stale_terminal_pnl_sol: config.rug_feedback_pnl_threshold_sol.unwrap_or(
                    loaded
                        .discovery
                        .rug_wallet_filter_max_stale_terminal_pnl_sol,
                ),
                limit: config.top,
            },
        )?;
        return serde_json::to_value(report).context("failed serializing rug feedback report");
    }
    let options =
        DiscoveryV2BuildOptions::from_config(&loaded.discovery, loaded.execution.enabled, now)
            .with_live_portfolio_rpc_url(live_portfolio_rpc_url_from_config(&loaded));
    let mut status = load_status(
        &store,
        &loaded.discovery,
        &loaded.shadow,
        options,
        config.live_rebuild,
    )?;
    status.shadow_signals_24h = Some(load_discovery_v2_shadow_signal_status(&store, now)?);
    let report = build_discovery_v2_wallet_report(
        &store,
        &loaded.discovery,
        &loaded.shadow,
        status,
        DiscoveryV2WalletReportOptions {
            now,
            limit: config.top,
            include_rejected: config.include_rejected,
        },
    )?;
    serde_json::to_value(report).context("failed serializing wallet report")
}

fn load_status(
    store: &SqliteDiscoveryStore,
    discovery: &copybot_config::DiscoveryConfig,
    shadow: &copybot_config::ShadowConfig,
    options: DiscoveryV2BuildOptions,
    live_rebuild: bool,
) -> Result<DiscoveryV2Status> {
    if live_rebuild {
        return build_discovery_v2_status(store, discovery, shadow, options);
    }
    load_materialized_discovery_v2_status_for_publish(store, discovery, shadow, &options)
        .map(|(status, _report)| status)
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
