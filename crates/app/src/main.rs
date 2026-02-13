use anyhow::{Context, Result};
use chrono::Utc;
use copybot_config::load_from_env_or_default;
use copybot_discovery::DiscoveryService;
use copybot_ingestion::IngestionService;
use copybot_shadow::{ShadowDropReason, ShadowProcessOutcome, ShadowService};
use copybot_storage::SqliteStore;
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use tokio::time::{self, Duration, MissedTickBehavior};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_CONFIG_PATH: &str = "configs/dev.toml";

#[tokio::main]
async fn main() -> Result<()> {
    let cli_config = parse_config_arg();
    let default_path = cli_config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    let (config, loaded_config_path) = load_from_env_or_default(&default_path)?;

    init_tracing(&config.system.log_level, config.system.log_json);
    info!(
        config_path = %loaded_config_path.display(),
        env = %config.system.env,
        "configuration loaded"
    );

    let mut store = SqliteStore::open(Path::new(&config.sqlite.path))
        .context("failed to initialize sqlite store")?;
    let migrations_dir = PathBuf::from(&config.system.migrations_dir);
    let applied = store
        .run_migrations(&migrations_dir)
        .with_context(|| format!("failed to apply migrations in {}", migrations_dir.display()))?;
    info!(applied, "sqlite migrations applied");

    store
        .record_heartbeat("copybot-app", "startup")
        .context("failed to write startup heartbeat")?;

    let ingestion = IngestionService::build(&config.ingestion)
        .context("failed to initialize ingestion service")?;
    let discovery = DiscoveryService::new_with_helius(
        config.discovery.clone(),
        config.shadow.clone(),
        Some(config.ingestion.helius_http_url.clone()),
    );
    let shadow = ShadowService::new_with_helius(
        config.shadow.clone(),
        Some(config.ingestion.helius_http_url.clone()),
    );

    run_app_loop(
        store,
        ingestion,
        discovery,
        shadow,
        config.system.heartbeat_seconds,
        config.discovery.refresh_seconds,
        config.shadow.refresh_seconds,
    )
    .await
}

fn parse_config_arg() -> Option<PathBuf> {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" {
            return args.next().map(PathBuf::from);
        }
        if let Some(inline) = arg.strip_prefix("--config=") {
            return Some(PathBuf::from(inline));
        }
    }
    None
}

fn init_tracing(log_level: &str, json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));
    if json {
        tracing_subscriber::fmt()
            .with_target(false)
            .with_env_filter(filter)
            .json()
            .compact()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_target(false)
            .with_env_filter(filter)
            .compact()
            .init();
    }
}

async fn run_app_loop(
    store: SqliteStore,
    mut ingestion: IngestionService,
    discovery: DiscoveryService,
    shadow: ShadowService,
    heartbeat_seconds: u64,
    discovery_refresh_seconds: u64,
    shadow_refresh_seconds: u64,
) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(heartbeat_seconds.max(1)));
    let mut discovery_interval =
        time::interval(Duration::from_secs(discovery_refresh_seconds.max(10)));
    let mut shadow_interval = time::interval(Duration::from_secs(shadow_refresh_seconds.max(10)));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    discovery_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    shadow_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut active_follow_wallets = store
        .list_active_follow_wallets()
        .context("failed to load active follow wallets")?;
    let mut shadow_drop_counts: BTreeMap<&'static str, u64> = BTreeMap::new();

    if !active_follow_wallets.is_empty() {
        info!(
            active_follow_wallets = active_follow_wallets.len(),
            "active follow wallets loaded"
        );
    }

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(error) = store.record_heartbeat("copybot-app", "alive") {
                    warn!(error = %error, "heartbeat write failed");
                }
            }
            maybe_swap = ingestion.next_swap() => {
                let swap = match maybe_swap {
                    Ok(Some(swap)) => swap,
                    Ok(None) => {
                        debug!("ingestion emitted no swap");
                        continue;
                    }
                    Err(error) => {
                        warn!(error = %error, "ingestion error");
                        continue;
                    }
                };

                match store.insert_observed_swap(&swap) {
                    Ok(true) => {
                        info!(
                            signature = %swap.signature,
                            wallet = %swap.wallet,
                            dex = %swap.dex,
                            token_in = %swap.token_in,
                            token_out = %swap.token_out,
                            amount_in = swap.amount_in,
                            amount_out = swap.amount_out,
                            "observed swap stored"
                        );
                        match shadow.process_swap(&store, &swap, &active_follow_wallets, Utc::now()) {
                            Ok(ShadowProcessOutcome::Recorded(result)) => {
                                info!(
                                    signal_id = %result.signal_id,
                                    wallet = %result.wallet_id,
                                    side = %result.side,
                                    token = %result.token,
                                    notional_sol = result.notional_sol,
                                    latency_ms = result.latency_ms,
                                    closed_qty = result.closed_qty,
                                    realized_pnl_sol = result.realized_pnl_sol,
                                    "shadow signal recorded"
                                );
                            }
                            Ok(ShadowProcessOutcome::Dropped(reason)) => {
                                let key = reason_to_key(reason);
                                *shadow_drop_counts.entry(key).or_insert(0) += 1;
                            }
                            Err(error) => {
                                warn!(error = %error, signature = %swap.signature, "shadow processing failed");
                            }
                        }
                    }
                    Ok(false) => {
                        debug!(signature = %swap.signature, "duplicate swap ignored");
                    }
                    Err(error) => {
                        warn!(error = %error, signature = %swap.signature, "failed writing observed swap");
                    }
                }
            }
            _ = discovery_interval.tick() => {
                let now = Utc::now();
                match discovery.run_cycle(&store, now) {
                    Ok(_) => match store.list_active_follow_wallets() {
                        Ok(wallets) => {
                            active_follow_wallets = wallets;
                        }
                        Err(error) => {
                            warn!(error = %error, "failed to refresh active follow wallets");
                        }
                    },
                    Err(error) => {
                        warn!(error = %error, "discovery cycle failed");
                    }
                }
            }
            _ = shadow_interval.tick() => {
                match shadow.snapshot_24h(&store, Utc::now()) {
                    Ok(snapshot) => {
                        info!(
                            closed_trades_24h = snapshot.closed_trades_24h,
                            realized_pnl_sol_24h = snapshot.realized_pnl_sol_24h,
                            open_lots = snapshot.open_lots,
                            active_follow_wallets = active_follow_wallets.len(),
                            "shadow snapshot"
                        );
                        if !shadow_drop_counts.is_empty() {
                            info!(
                                drop_counts = ?shadow_drop_counts,
                                "shadow drop reasons"
                            );
                            shadow_drop_counts.clear();
                        }
                    }
                    Err(error) => {
                        warn!(error = %error, "shadow snapshot failed");
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signal received");
                break;
            }
        }
    }

    store
        .record_heartbeat("copybot-app", "shutdown")
        .context("failed to write shutdown heartbeat")?;
    Ok(())
}

fn reason_to_key(reason: ShadowDropReason) -> &'static str {
    reason.as_str()
}
