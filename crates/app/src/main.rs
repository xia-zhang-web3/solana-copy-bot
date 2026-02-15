use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_env_or_default;
use copybot_core_types::SwapEvent;
use copybot_discovery::DiscoveryService;
use copybot_ingestion::IngestionService;
use copybot_shadow::{
    FollowSnapshot, ShadowDropReason, ShadowProcessOutcome, ShadowService, ShadowSnapshot,
};
use copybot_storage::SqliteStore;
use std::collections::{BTreeMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, MissedTickBehavior};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_CONFIG_PATH: &str = "configs/dev.toml";
const SHADOW_WRITE_QUEUE_CAPACITY: usize = 256;
const OBSERVED_SWAP_WRITE_MAX_RETRIES: usize = 3;
const OBSERVED_SWAP_RETRY_BACKOFF_MS: [u64; OBSERVED_SWAP_WRITE_MAX_RETRIES] = [50, 125, 250];

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
    let migrations_dir = resolve_migrations_dir(&loaded_config_path, &config.system.migrations_dir);
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
        config.sqlite.path.clone(),
        config.system.heartbeat_seconds,
        config.discovery.refresh_seconds,
        config.shadow.refresh_seconds,
        config.shadow.max_signal_lag_seconds,
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

fn resolve_migrations_dir(config_path: &Path, configured_migrations_dir: &str) -> PathBuf {
    let configured = PathBuf::from(configured_migrations_dir);
    if configured.is_absolute() || configured.exists() {
        return configured;
    }

    if let Some(config_parent) = config_path.parent() {
        let sibling_candidate = config_parent.join(&configured);
        if sibling_candidate.exists() {
            return sibling_candidate;
        }

        if let Some(project_root) = config_parent.parent() {
            let root_candidate = project_root.join(&configured);
            if root_candidate.exists() {
                return root_candidate;
            }
        }
    }

    configured
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
    sqlite_path: String,
    heartbeat_seconds: u64,
    discovery_refresh_seconds: u64,
    shadow_refresh_seconds: u64,
    shadow_max_signal_lag_seconds: u64,
) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(heartbeat_seconds.max(1)));
    let mut discovery_interval =
        time::interval(Duration::from_secs(discovery_refresh_seconds.max(10)));
    let mut shadow_interval = time::interval(Duration::from_secs(shadow_refresh_seconds.max(10)));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    discovery_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    shadow_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let initial_active_wallets = store
        .list_active_follow_wallets()
        .context("failed to load active follow wallets")?;
    let mut follow_snapshot = FollowSnapshot::from_active_wallets(initial_active_wallets);
    let follow_event_retention = Duration::from_secs(shadow_max_signal_lag_seconds.max(1));
    let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut discovery_handle: Option<JoinHandle<Result<DiscoveryTaskOutput>>> = None;
    let mut shadow_handle: Option<JoinHandle<ShadowTaskOutput>> = None;
    let mut shadow_snapshot_handle: Option<JoinHandle<Result<ShadowSnapshot>>> = None;
    let mut shadow_store: Option<SqliteStore> = None;
    let (shadow_task_tx, mut shadow_task_rx) =
        tokio::sync::mpsc::channel::<ShadowTaskInput>(SHADOW_WRITE_QUEUE_CAPACITY);

    if !follow_snapshot.active.is_empty() {
        info!(
            active_follow_wallets = follow_snapshot.active.len(),
            "active follow wallets loaded"
        );
    }

    loop {
        spawn_next_shadow_task_if_idle(
            &mut shadow_handle,
            &mut shadow_task_rx,
            &mut shadow_store,
            &sqlite_path,
            &shadow,
        );

        tokio::select! {
            discovery_result = async {
                if let Some(handle) = &mut discovery_handle {
                    Some(handle.await)
                } else {
                    None
                }
            }, if discovery_handle.is_some() => {
                discovery_handle = None;
                match discovery_result.expect("guard ensures discovery task exists") {
                    Ok(Ok(discovery_output)) => {
                        apply_follow_snapshot_update(
                            &mut follow_snapshot,
                            discovery_output.active_wallets,
                            discovery_output.cycle_ts,
                            follow_event_retention,
                        );
                    }
                    Ok(Err(error)) => {
                        warn!(error = %error, "discovery cycle failed");
                    }
                    Err(error) => {
                        warn!(error = %error, "discovery task join failed");
                    }
                }
            }
            shadow_result = async {
                if let Some(handle) = &mut shadow_handle {
                    Some(handle.await)
                } else {
                    None
                }
            }, if shadow_handle.is_some() => {
                shadow_handle = None;
                match shadow_result.expect("guard ensures shadow task exists") {
                    Ok(task_output) => {
                        shadow_store = Some(task_output.store);
                        match task_output.outcome {
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
                                let reason_key = reason_to_key(reason);
                                let stage_key = reason_to_stage(reason);
                                *shadow_drop_reason_counts.entry(reason_key).or_insert(0) += 1;
                                *shadow_drop_stage_counts.entry(stage_key).or_insert(0) += 1;
                            }
                            Err(error) => {
                                warn!(
                                    error = %error,
                                    signature = %task_output.signature,
                                    "shadow processing failed"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        shadow_store = None;
                        warn!(error = %error, "shadow task join failed");
                    }
                }
            }
            snapshot_result = async {
                if let Some(handle) = &mut shadow_snapshot_handle {
                    Some(handle.await)
                } else {
                    None
                }
            }, if shadow_snapshot_handle.is_some() => {
                shadow_snapshot_handle = None;
                match snapshot_result.expect("guard ensures shadow snapshot task exists") {
                    Ok(Ok(snapshot)) => {
                        info!(
                            closed_trades_24h = snapshot.closed_trades_24h,
                            realized_pnl_sol_24h = snapshot.realized_pnl_sol_24h,
                            open_lots = snapshot.open_lots,
                            active_follow_wallets = follow_snapshot.active.len(),
                            "shadow snapshot"
                        );
                        if !shadow_drop_reason_counts.is_empty() {
                            info!(
                                drop_counts = ?shadow_drop_reason_counts,
                                drop_stage_counts = ?shadow_drop_stage_counts,
                                "shadow drop reasons"
                            );
                            shadow_drop_reason_counts.clear();
                            shadow_drop_stage_counts.clear();
                        }
                    }
                    Ok(Err(error)) => {
                        warn!(error = %error, "shadow snapshot failed");
                    }
                    Err(error) => {
                        warn!(error = %error, "shadow snapshot task join failed");
                    }
                }
            }
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

                match insert_observed_swap_with_retry(&store, &swap).await {
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

                        if matches!(classify_swap_side(&swap), Some(ShadowSwapSide::Buy))
                            && !follow_snapshot.is_followed_at(&swap.wallet, swap.ts_utc)
                        {
                            let reason = "not_followed";
                            let runtime_followed = follow_snapshot.is_active(&swap.wallet);
                            *shadow_drop_reason_counts.entry(reason).or_insert(0) += 1;
                            *shadow_drop_stage_counts.entry("follow").or_insert(0) += 1;
                            warn!(
                                stage = "follow",
                                reason,
                                side = "buy",
                                wallet = %swap.wallet,
                                token_in = %swap.token_in,
                                token_out = %swap.token_out,
                                runtime_followed,
                                signature = %swap.signature,
                                "shadow gate dropped"
                            );
                            continue;
                        }

                        let task_input = ShadowTaskInput {
                            swap,
                            follow_snapshot: follow_snapshot.clone(),
                        };
                        match shadow_task_tx.try_send(task_input) {
                            Ok(()) => {}
                            Err(tokio::sync::mpsc::error::TrySendError::Full(returned)) => {
                                let swap = returned.swap;
                                let reason = "queue_saturated";
                                *shadow_drop_reason_counts.entry(reason).or_insert(0) += 1;
                                *shadow_drop_stage_counts.entry("queue").or_insert(0) += 1;
                                warn!(
                                    stage = "queue",
                                    reason,
                                    wallet = %swap.wallet,
                                    token_in = %swap.token_in,
                                    token_out = %swap.token_out,
                                    queue_capacity = SHADOW_WRITE_QUEUE_CAPACITY,
                                    signature = %swap.signature,
                                    "shadow gate dropped"
                                );
                                let details_json = format!(
                                    "{{\"reason\":\"{reason}\",\"signature\":\"{}\",\"wallet\":\"{}\",\"queue_capacity\":{}}}",
                                    swap.signature,
                                    swap.wallet,
                                    SHADOW_WRITE_QUEUE_CAPACITY,
                                );
                                if let Err(error) = store.insert_risk_event(
                                    "shadow_queue_saturated",
                                    "warn",
                                    Utc::now(),
                                    Some(&details_json),
                                ) {
                                    warn!(
                                        error = %error,
                                        signature = %swap.signature,
                                        "failed to persist shadow queue saturation risk event"
                                    );
                                }
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(returned)) => {
                                warn!(
                                    signature = %returned.swap.signature,
                                    wallet = %returned.swap.wallet,
                                    "shadow queue closed, dropping signal candidate"
                                );
                            }
                        }
                    }
                    Ok(false) => {
                        debug!(signature = %swap.signature, "duplicate swap ignored");
                    }
                    Err(error) => {
                        let error_chain = format_error_chain(&error);
                        warn!(
                            error = %error,
                            error_chain = %error_chain,
                            signature = %swap.signature,
                            "failed writing observed swap"
                        );
                    }
                }
            }
            _ = discovery_interval.tick() => {
                if discovery_handle.is_some() {
                    warn!("discovery cycle still running, skipping scheduled trigger");
                    continue;
                }
                discovery_handle = Some(tokio::task::spawn_blocking(spawn_discovery_task(
                    sqlite_path.clone(),
                    discovery.clone(),
                    Utc::now(),
                )));
            }
            _ = shadow_interval.tick() => {
                if shadow_snapshot_handle.is_some() {
                    warn!("shadow snapshot still running, skipping scheduled trigger");
                    continue;
                }
                shadow_snapshot_handle = Some(tokio::task::spawn_blocking(spawn_shadow_snapshot_task(
                    sqlite_path.clone(),
                    shadow.clone(),
                    Utc::now(),
                )));
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

fn reason_to_stage(reason: ShadowDropReason) -> &'static str {
    match reason {
        ShadowDropReason::Disabled => "disabled",
        ShadowDropReason::NotFollowed => "follow",
        ShadowDropReason::NotSolLeg => "pair",
        ShadowDropReason::BelowNotional => "notional",
        ShadowDropReason::LagExceeded => "lag",
        ShadowDropReason::TooNew
        | ShadowDropReason::LowHolders
        | ShadowDropReason::LowLiquidity
        | ShadowDropReason::LowVolume
        | ShadowDropReason::ThinMarket => "quality",
        ShadowDropReason::InvalidSizing => "sizing",
        ShadowDropReason::DuplicateSignal => "dedupe",
        ShadowDropReason::UnsupportedSide => "side",
    }
}

async fn insert_observed_swap_with_retry(store: &SqliteStore, swap: &SwapEvent) -> Result<bool> {
    for attempt in 0..=OBSERVED_SWAP_WRITE_MAX_RETRIES {
        match store.insert_observed_swap(swap) {
            Ok(written) => return Ok(written),
            Err(error) => {
                if attempt < OBSERVED_SWAP_WRITE_MAX_RETRIES && is_retryable_sqlite_error(&error) {
                    let backoff_ms = OBSERVED_SWAP_RETRY_BACKOFF_MS[attempt];
                    debug!(
                        signature = %swap.signature,
                        attempt = attempt + 1,
                        max_attempts = OBSERVED_SWAP_WRITE_MAX_RETRIES + 1,
                        backoff_ms,
                        error = %error,
                        "retrying observed swap write after sqlite contention"
                    );
                    time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                return Err(error);
            }
        }
    }
    unreachable!("retry loop must return on success or terminal error");
}

fn is_retryable_sqlite_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let lowered = cause.to_string().to_ascii_lowercase();
        lowered.contains("database is locked")
            || lowered.contains("database is busy")
            || lowered.contains("database table is locked")
    })
}

fn format_error_chain(error: &anyhow::Error) -> String {
    let mut chain = String::new();
    for (idx, cause) in error.chain().enumerate() {
        if idx > 0 {
            chain.push_str(" | ");
        }
        chain.push_str(&cause.to_string());
    }
    chain
}

fn spawn_discovery_task(
    sqlite_path: String,
    discovery: DiscoveryService,
    now: chrono::DateTime<Utc>,
) -> impl FnOnce() -> Result<DiscoveryTaskOutput> {
    move || {
        let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for discovery task: {sqlite_path}")
        })?;
        discovery.run_cycle(&store, now)?;
        let active_wallets = store.list_active_follow_wallets()?;
        Ok(DiscoveryTaskOutput {
            active_wallets,
            cycle_ts: now,
        })
    }
}

fn spawn_shadow_snapshot_task(
    sqlite_path: String,
    shadow: ShadowService,
    now: chrono::DateTime<Utc>,
) -> impl FnOnce() -> Result<ShadowSnapshot> {
    move || {
        let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for shadow snapshot task: {sqlite_path}")
        })?;
        shadow.snapshot_24h(&store, now)
    }
}

struct ShadowTaskOutput {
    signature: String,
    outcome: Result<ShadowProcessOutcome>,
    store: SqliteStore,
}

struct DiscoveryTaskOutput {
    active_wallets: HashSet<String>,
    cycle_ts: DateTime<Utc>,
}

struct ShadowTaskInput {
    swap: SwapEvent,
    follow_snapshot: FollowSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShadowSwapSide {
    Buy,
    Sell,
}

fn classify_swap_side(swap: &SwapEvent) -> Option<ShadowSwapSide> {
    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
    if swap.token_in == SOL_MINT && swap.token_out != SOL_MINT {
        return Some(ShadowSwapSide::Buy);
    }
    if swap.token_out == SOL_MINT && swap.token_in != SOL_MINT {
        return Some(ShadowSwapSide::Sell);
    }
    None
}

fn apply_follow_snapshot_update(
    follow_snapshot: &mut FollowSnapshot,
    active_wallets: HashSet<String>,
    cycle_ts: DateTime<Utc>,
    retention: Duration,
) {
    let promoted_wallets: Vec<String> = active_wallets
        .difference(&follow_snapshot.active)
        .cloned()
        .collect();
    let demoted_wallets: Vec<String> = follow_snapshot
        .active
        .difference(&active_wallets)
        .cloned()
        .collect();

    for wallet in promoted_wallets {
        follow_snapshot.promoted_at.insert(wallet, cycle_ts);
    }
    for wallet in demoted_wallets {
        follow_snapshot.demoted_at.insert(wallet, cycle_ts);
    }
    follow_snapshot.active = active_wallets;

    let cutoff = cycle_ts - chrono::Duration::seconds(retention.as_secs() as i64);
    follow_snapshot.promoted_at.retain(|_, ts| *ts >= cutoff);
    follow_snapshot.demoted_at.retain(|_, ts| *ts >= cutoff);
}

fn spawn_next_shadow_task_if_idle(
    shadow_handle: &mut Option<JoinHandle<ShadowTaskOutput>>,
    shadow_task_rx: &mut tokio::sync::mpsc::Receiver<ShadowTaskInput>,
    shadow_store: &mut Option<SqliteStore>,
    sqlite_path: &str,
    shadow: &ShadowService,
) {
    if shadow_handle.is_some() {
        return;
    }
    let next = match shadow_task_rx.try_recv() {
        Ok(task_input) => task_input,
        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return,
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
            warn!("shadow task channel disconnected");
            return;
        }
    };

    let store = match shadow_store.take() {
        Some(store) => store,
        None => match SqliteStore::open(Path::new(sqlite_path)) {
            Ok(store) => store,
            Err(error) => {
                warn!(
                    error = %error,
                    sqlite_path,
                    "failed to open sqlite db for shadow worker"
                );
                return;
            }
        },
    };
    *shadow_handle = Some(tokio::task::spawn_blocking(shadow_task(
        shadow.clone(),
        store,
        next,
    )));
}

fn shadow_task(
    shadow: ShadowService,
    store: SqliteStore,
    task_input: ShadowTaskInput,
) -> impl FnOnce() -> ShadowTaskOutput {
    move || {
        let ShadowTaskInput {
            swap,
            follow_snapshot,
        } = task_input;
        let signature = swap.signature.clone();
        let outcome = shadow.process_swap(&store, &swap, &follow_snapshot, Utc::now());
        ShadowTaskOutput {
            signature,
            outcome,
            store,
        }
    }
}
