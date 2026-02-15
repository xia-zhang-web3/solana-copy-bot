use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_env_or_default;
use copybot_core_types::SwapEvent;
use copybot_discovery::DiscoveryService;
use copybot_ingestion::IngestionService;
use copybot_shadow::{
    FollowSnapshot, ShadowDropReason, ShadowProcessOutcome, ShadowService, ShadowSnapshot,
};
use copybot_storage::{is_retryable_sqlite_anyhow_error, SqliteStore};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{self, Duration, MissedTickBehavior};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_CONFIG_PATH: &str = "configs/dev.toml";
const SHADOW_WORKER_POOL_SIZE: usize = 4;
const SHADOW_INLINE_WORKER_OVERFLOW: usize = 1;
const SHADOW_MAX_CONCURRENT_WORKERS: usize =
    SHADOW_WORKER_POOL_SIZE + SHADOW_INLINE_WORKER_OVERFLOW;
const SHADOW_PENDING_TASK_CAPACITY: usize = 256;
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
    let mut follow_snapshot = Arc::new(FollowSnapshot::from_active_wallets(initial_active_wallets));
    let follow_event_retention = Duration::from_secs(shadow_max_signal_lag_seconds.max(1));
    let mut open_shadow_lots = store
        .list_shadow_open_pairs()
        .context("failed to load open shadow lot index")?;
    let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut discovery_handle: Option<JoinHandle<Result<DiscoveryTaskOutput>>> = None;
    let mut shadow_workers: JoinSet<ShadowTaskOutput> = JoinSet::new();
    let mut shadow_snapshot_handle: Option<JoinHandle<Result<ShadowSnapshot>>> = None;
    let mut pending_shadow_tasks: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> =
        HashMap::new();
    let mut pending_shadow_task_count: usize = 0;
    let mut ready_shadow_keys: VecDeque<ShadowTaskKey> = VecDeque::new();
    let mut ready_shadow_key_set: HashSet<ShadowTaskKey> = HashSet::new();
    let mut inflight_shadow_keys: HashSet<ShadowTaskKey> = HashSet::new();
    let mut shadow_queue_backpressure_active = false;
    let mut shadow_scheduler_needs_reset = false;

    if !follow_snapshot.active.is_empty() {
        info!(
            active_follow_wallets = follow_snapshot.active.len(),
            "active follow wallets loaded"
        );
    }

    loop {
        if shadow_scheduler_needs_reset {
            if shadow_workers.is_empty() {
                inflight_shadow_keys.clear();
                rebuild_shadow_ready_queue(
                    &pending_shadow_tasks,
                    &mut ready_shadow_keys,
                    &mut ready_shadow_key_set,
                    &inflight_shadow_keys,
                );
                shadow_scheduler_needs_reset = false;
                warn!("shadow scheduler recovered after worker join error");
            }
        } else {
            spawn_shadow_tasks_up_to_limit(
                &mut shadow_workers,
                &mut pending_shadow_tasks,
                &mut pending_shadow_task_count,
                &mut ready_shadow_keys,
                &mut ready_shadow_key_set,
                &mut inflight_shadow_keys,
                &sqlite_path,
                &shadow,
                SHADOW_WORKER_POOL_SIZE,
            );
        }

        let shadow_queue_full = pending_shadow_task_count >= SHADOW_PENDING_TASK_CAPACITY;
        if shadow_queue_full && !shadow_queue_backpressure_active {
            shadow_queue_backpressure_active = true;
            warn!(
                pending_shadow_task_count,
                shadow_pending_capacity = SHADOW_PENDING_TASK_CAPACITY,
                "shadow queue backpressure active; switching to inline shadow processing"
            );
            let details_json = format!(
                "{{\"reason\":\"queue_backpressure\",\"pending\":{},\"capacity\":{}}}",
                pending_shadow_task_count, SHADOW_PENDING_TASK_CAPACITY
            );
            if let Err(error) = store.insert_risk_event(
                "shadow_queue_saturated",
                "warn",
                Utc::now(),
                Some(&details_json),
            ) {
                warn!(
                    error = %error,
                    "failed to persist shadow queue backpressure risk event"
                );
            }
        } else if !shadow_queue_full && shadow_queue_backpressure_active {
            shadow_queue_backpressure_active = false;
            info!(
                pending_shadow_task_count,
                shadow_pending_capacity = SHADOW_PENDING_TASK_CAPACITY,
                "shadow queue backpressure cleared"
            );
        }

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
                        let mut snapshot = (*follow_snapshot).clone();
                        apply_follow_snapshot_update(
                            &mut snapshot,
                            discovery_output.active_wallets,
                            discovery_output.cycle_ts,
                            follow_event_retention,
                        );
                        follow_snapshot = Arc::new(snapshot);
                    }
                    Ok(Err(error)) => {
                        warn!(error = %error, "discovery cycle failed");
                    }
                    Err(error) => {
                        warn!(error = %error, "discovery task join failed");
                    }
                }
            }
            shadow_result = shadow_workers.join_next(), if !shadow_workers.is_empty() => {
                match shadow_result {
                    Some(Ok(task_output)) => {
                        mark_shadow_task_complete(
                            &task_output.key,
                            &pending_shadow_tasks,
                            &mut ready_shadow_keys,
                            &mut ready_shadow_key_set,
                            &mut inflight_shadow_keys,
                        );
                        handle_shadow_task_output(
                            task_output,
                            &mut open_shadow_lots,
                            &mut shadow_drop_reason_counts,
                            &mut shadow_drop_stage_counts,
                        );
                    }
                    Some(Err(error)) => {
                        warn!(error = %error, "shadow task join failed");
                        shadow_scheduler_needs_reset = true;
                    }
                    None => {}
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
                        match store.list_shadow_open_pairs() {
                            Ok(pairs) => {
                                open_shadow_lots = pairs;
                            }
                            Err(error) => {
                                warn!(error = %error, "failed to refresh open shadow lot index");
                            }
                        }
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
                        if !shadow_queue_full_outcome_counts.is_empty() {
                            info!(
                                queue_full_outcomes = ?shadow_queue_full_outcome_counts,
                                "shadow queue_full outcomes"
                            );
                            shadow_queue_full_outcome_counts.clear();
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

                        let Some(side) = classify_swap_side(&swap) else {
                            continue;
                        };
                        if matches!(side, ShadowSwapSide::Buy)
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

                        if matches!(side, ShadowSwapSide::Sell)
                            && !follow_snapshot.is_followed_at(&swap.wallet, swap.ts_utc)
                        {
                            let wallet_has_recent_follow_history = follow_snapshot.is_active(&swap.wallet)
                                || follow_snapshot.promoted_at.contains_key(&swap.wallet)
                                || follow_snapshot.demoted_at.contains_key(&swap.wallet);
                            let sell_key = shadow_task_key_for_swap(&swap, side);
                            let key_tuple = (sell_key.wallet.clone(), sell_key.token.clone());
                            let has_pending_or_inflight = key_has_pending_or_inflight(
                                &sell_key,
                                &pending_shadow_tasks,
                                &inflight_shadow_keys,
                            );
                            if !wallet_has_recent_follow_history
                                && !has_pending_or_inflight
                                && !open_shadow_lots.contains(&key_tuple)
                            {
                                let reason = "not_followed";
                                *shadow_drop_reason_counts.entry(reason).or_insert(0) += 1;
                                *shadow_drop_stage_counts.entry("follow").or_insert(0) += 1;
                                warn!(
                                    stage = "follow",
                                    reason,
                                    side = "sell",
                                    wallet = %swap.wallet,
                                    token = %swap.token_in,
                                    signature = %swap.signature,
                                    "shadow gate dropped"
                                );
                                continue;
                            }
                        }

                        let task_key = shadow_task_key_for_swap(&swap, side);
                        let task_input = ShadowTaskInput {
                            swap,
                            follow_snapshot: Arc::clone(&follow_snapshot),
                            key: task_key,
                        };
                        if should_process_shadow_inline(
                            shadow_queue_full,
                            shadow_scheduler_needs_reset,
                            shadow_workers.len(),
                            &task_input.key,
                            &pending_shadow_tasks,
                            &inflight_shadow_keys,
                        ) {
                            if inflight_shadow_keys.insert(task_input.key.clone()) {
                                spawn_shadow_worker_task(
                                    &mut shadow_workers,
                                    &shadow,
                                    &sqlite_path,
                                    task_input,
                                );
                            } else {
                                if let Err(dropped_task) = enqueue_shadow_task(
                                    &mut pending_shadow_tasks,
                                    &mut pending_shadow_task_count,
                                    &mut ready_shadow_keys,
                                    &mut ready_shadow_key_set,
                                    &inflight_shadow_keys,
                                    SHADOW_PENDING_TASK_CAPACITY,
                                    task_input,
                                ) {
                                    handle_shadow_enqueue_overflow(
                                        side,
                                        dropped_task,
                                        &mut pending_shadow_tasks,
                                        &mut pending_shadow_task_count,
                                        &mut ready_shadow_keys,
                                        &mut ready_shadow_key_set,
                                        &inflight_shadow_keys,
                                        SHADOW_PENDING_TASK_CAPACITY,
                                        &mut shadow_drop_reason_counts,
                                        &mut shadow_drop_stage_counts,
                                        &mut shadow_queue_full_outcome_counts,
                                    );
                                }
                            }
                        } else {
                            if let Err(dropped_task) = enqueue_shadow_task(
                                &mut pending_shadow_tasks,
                                &mut pending_shadow_task_count,
                                &mut ready_shadow_keys,
                                &mut ready_shadow_key_set,
                                &inflight_shadow_keys,
                                SHADOW_PENDING_TASK_CAPACITY,
                                task_input,
                            ) {
                                handle_shadow_enqueue_overflow(
                                    side,
                                    dropped_task,
                                    &mut pending_shadow_tasks,
                                    &mut pending_shadow_task_count,
                                    &mut ready_shadow_keys,
                                    &mut ready_shadow_key_set,
                                    &inflight_shadow_keys,
                                    SHADOW_PENDING_TASK_CAPACITY,
                                    &mut shadow_drop_reason_counts,
                                    &mut shadow_drop_stage_counts,
                                    &mut shadow_queue_full_outcome_counts,
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
                if attempt < OBSERVED_SWAP_WRITE_MAX_RETRIES
                    && is_retryable_sqlite_anyhow_error(&error)
                {
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
    key: ShadowTaskKey,
    outcome: Result<ShadowProcessOutcome>,
}

struct DiscoveryTaskOutput {
    active_wallets: HashSet<String>,
    cycle_ts: DateTime<Utc>,
}

struct ShadowTaskInput {
    swap: SwapEvent,
    follow_snapshot: Arc<FollowSnapshot>,
    key: ShadowTaskKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ShadowTaskKey {
    wallet: String,
    token: String,
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

fn shadow_task_key_for_swap(swap: &SwapEvent, side: ShadowSwapSide) -> ShadowTaskKey {
    match side {
        ShadowSwapSide::Buy => ShadowTaskKey {
            wallet: swap.wallet.clone(),
            token: swap.token_out.clone(),
        },
        ShadowSwapSide::Sell => ShadowTaskKey {
            wallet: swap.wallet.clone(),
            token: swap.token_in.clone(),
        },
    }
}

fn key_has_pending_or_inflight(
    key: &ShadowTaskKey,
    pending_shadow_tasks: &HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    inflight_shadow_keys: &HashSet<ShadowTaskKey>,
) -> bool {
    inflight_shadow_keys.contains(key)
        || pending_shadow_tasks
            .get(key)
            .is_some_and(|pending| !pending.is_empty())
}

fn should_process_shadow_inline(
    shadow_queue_full: bool,
    shadow_scheduler_needs_reset: bool,
    shadow_worker_count: usize,
    key: &ShadowTaskKey,
    pending_shadow_tasks: &HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    inflight_shadow_keys: &HashSet<ShadowTaskKey>,
) -> bool {
    shadow_queue_full
        && !shadow_scheduler_needs_reset
        && shadow_worker_count < SHADOW_MAX_CONCURRENT_WORKERS
        && !key_has_pending_or_inflight(key, pending_shadow_tasks, inflight_shadow_keys)
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

fn spawn_shadow_worker_task(
    shadow_workers: &mut JoinSet<ShadowTaskOutput>,
    shadow: &ShadowService,
    sqlite_path: &str,
    task_input: ShadowTaskInput,
) {
    let shadow = shadow.clone();
    let sqlite_path = sqlite_path.to_string();
    let fallback_signature = task_input.swap.signature.clone();
    let fallback_key = task_input.key.clone();
    shadow_workers.spawn_blocking(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            shadow_task(shadow, &sqlite_path, task_input)
        }));
        match result {
            Ok(output) => output,
            Err(payload) => ShadowTaskOutput {
                signature: fallback_signature,
                key: fallback_key,
                outcome: Err(anyhow::anyhow!(
                    "shadow worker task panicked: {}",
                    panic_payload_to_string(payload.as_ref())
                )),
            },
        }
    });
}

fn spawn_shadow_tasks_up_to_limit(
    shadow_workers: &mut JoinSet<ShadowTaskOutput>,
    pending_shadow_tasks: &mut HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    pending_shadow_task_count: &mut usize,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &mut HashSet<ShadowTaskKey>,
    sqlite_path: &str,
    shadow: &ShadowService,
    max_workers: usize,
) {
    while shadow_workers.len() < max_workers {
        let Some(next) = dequeue_next_shadow_task(
            pending_shadow_tasks,
            pending_shadow_task_count,
            ready_shadow_keys,
            ready_shadow_key_set,
            inflight_shadow_keys,
        ) else {
            return;
        };
        spawn_shadow_worker_task(shadow_workers, shadow, sqlite_path, next);
    }
}

fn enqueue_shadow_task(
    pending_shadow_tasks: &mut HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    pending_shadow_task_count: &mut usize,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &HashSet<ShadowTaskKey>,
    capacity: usize,
    task_input: ShadowTaskInput,
) -> std::result::Result<(), ShadowTaskInput> {
    if *pending_shadow_task_count >= capacity {
        return Err(task_input);
    }
    let key = task_input.key.clone();
    pending_shadow_tasks
        .entry(key.clone())
        .or_default()
        .push_back(task_input);
    *pending_shadow_task_count = pending_shadow_task_count.saturating_add(1);
    if !inflight_shadow_keys.contains(&key) && ready_shadow_key_set.insert(key.clone()) {
        ready_shadow_keys.push_back(key);
    }
    Ok(())
}

fn handle_shadow_enqueue_overflow(
    overflow_side: ShadowSwapSide,
    overflow_task: ShadowTaskInput,
    pending_shadow_tasks: &mut HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    pending_shadow_task_count: &mut usize,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &HashSet<ShadowTaskKey>,
    capacity: usize,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
) {
    match overflow_side {
        ShadowSwapSide::Buy => {
            record_shadow_queue_full_buy_drop(
                &overflow_task.swap,
                shadow_drop_reason_counts,
                shadow_drop_stage_counts,
                shadow_queue_full_outcome_counts,
            );
        }
        ShadowSwapSide::Sell => {
            if let Some(evicted_buy_task) = evict_one_pending_buy_task(
                pending_shadow_tasks,
                pending_shadow_task_count,
                ready_shadow_keys,
                ready_shadow_key_set,
            ) {
                let sell_swap_for_log = overflow_task.swap.clone();
                match enqueue_shadow_task(
                    pending_shadow_tasks,
                    pending_shadow_task_count,
                    ready_shadow_keys,
                    ready_shadow_key_set,
                    inflight_shadow_keys,
                    capacity,
                    overflow_task,
                ) {
                    Ok(()) => {
                        record_shadow_queue_full_buy_drop(
                            &evicted_buy_task.swap,
                            shadow_drop_reason_counts,
                            shadow_drop_stage_counts,
                            shadow_queue_full_outcome_counts,
                        );
                        record_shadow_queue_full_sell_outcome(
                            &sell_swap_for_log,
                            true,
                            shadow_drop_reason_counts,
                            shadow_drop_stage_counts,
                            shadow_queue_full_outcome_counts,
                        );
                    }
                    Err(dropped_sell_task) => {
                        if let Err(still_evicted_buy_task) = enqueue_shadow_task(
                            pending_shadow_tasks,
                            pending_shadow_task_count,
                            ready_shadow_keys,
                            ready_shadow_key_set,
                            inflight_shadow_keys,
                            capacity,
                            evicted_buy_task,
                        ) {
                            record_shadow_queue_full_buy_drop(
                                &still_evicted_buy_task.swap,
                                shadow_drop_reason_counts,
                                shadow_drop_stage_counts,
                                shadow_queue_full_outcome_counts,
                            );
                        }
                        record_shadow_queue_full_sell_outcome(
                            &dropped_sell_task.swap,
                            false,
                            shadow_drop_reason_counts,
                            shadow_drop_stage_counts,
                            shadow_queue_full_outcome_counts,
                        );
                    }
                }
            } else {
                record_shadow_queue_full_sell_outcome(
                    &overflow_task.swap,
                    false,
                    shadow_drop_reason_counts,
                    shadow_drop_stage_counts,
                    shadow_queue_full_outcome_counts,
                );
            }
        }
    }
}

fn evict_one_pending_buy_task(
    pending_shadow_tasks: &mut HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    pending_shadow_task_count: &mut usize,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
) -> Option<ShadowTaskInput> {
    let ready_candidate = ready_shadow_keys.iter().find_map(|key| {
        pending_shadow_tasks
            .get(key)
            .and_then(find_last_pending_buy_index)
            .map(|index| (key.clone(), index))
    });
    let candidate = ready_candidate.or_else(|| {
        let keys: Vec<ShadowTaskKey> = pending_shadow_tasks.keys().cloned().collect();
        keys.into_iter().find_map(|key| {
            pending_shadow_tasks
                .get(&key)
                .and_then(find_last_pending_buy_index)
                .map(|index| (key, index))
        })
    })?;

    let (key, index) = candidate;
    let mut remove_key = false;
    let removed = if let Some(queue) = pending_shadow_tasks.get_mut(&key) {
        let task = queue.remove(index);
        remove_key = queue.is_empty();
        task
    } else {
        None
    };

    if remove_key {
        pending_shadow_tasks.remove(&key);
        ready_shadow_key_set.remove(&key);
        ready_shadow_keys.retain(|ready_key| ready_key != &key);
    }

    if removed.is_some() {
        *pending_shadow_task_count = pending_shadow_task_count.saturating_sub(1);
    }
    removed
}

fn find_last_pending_buy_index(queue: &VecDeque<ShadowTaskInput>) -> Option<usize> {
    (0..queue.len()).rev().find(|index| {
        queue
            .get(*index)
            .and_then(|task| classify_swap_side(&task.swap))
            .is_some_and(|side| matches!(side, ShadowSwapSide::Buy))
    })
}

fn record_shadow_queue_full_buy_drop(
    swap: &SwapEvent,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
) {
    let reason = "queue_full_buy_drop";
    *shadow_drop_reason_counts.entry(reason).or_insert(0) += 1;
    *shadow_drop_stage_counts.entry("scheduler").or_insert(0) += 1;
    *shadow_queue_full_outcome_counts.entry(reason).or_insert(0) += 1;
    warn!(
        stage = "scheduler",
        reason,
        side = "buy",
        wallet = %swap.wallet,
        token = %swap.token_out,
        signature = %swap.signature,
        "shadow gate dropped"
    );
}

fn record_shadow_queue_full_sell_outcome(
    swap: &SwapEvent,
    kept: bool,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
    shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
) {
    let reason = "queue_full_sell_kept_or_dropped";
    let outcome_key = if kept {
        "queue_full_sell_kept"
    } else {
        "queue_full_sell_dropped"
    };
    *shadow_queue_full_outcome_counts
        .entry(outcome_key)
        .or_insert(0) += 1;
    if kept {
        info!(
            stage = "scheduler",
            reason,
            outcome = "kept",
            side = "sell",
            wallet = %swap.wallet,
            token = %swap.token_in,
            signature = %swap.signature,
            "shadow queue_full sell outcome"
        );
    } else {
        *shadow_drop_reason_counts
            .entry("queue_full_sell_dropped")
            .or_insert(0) += 1;
        *shadow_drop_stage_counts.entry("scheduler").or_insert(0) += 1;
        warn!(
            stage = "scheduler",
            reason,
            outcome = "dropped",
            side = "sell",
            wallet = %swap.wallet,
            token = %swap.token_in,
            signature = %swap.signature,
            "shadow gate dropped"
        );
    }
}

fn dequeue_next_shadow_task(
    pending_shadow_tasks: &mut HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    pending_shadow_task_count: &mut usize,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &mut HashSet<ShadowTaskKey>,
) -> Option<ShadowTaskInput> {
    while let Some(key) = ready_shadow_keys.pop_front() {
        ready_shadow_key_set.remove(&key);
        if inflight_shadow_keys.contains(&key) {
            continue;
        }

        let mut remove_key = false;
        let next_task = if let Some(queue) = pending_shadow_tasks.get_mut(&key) {
            let task = queue.pop_front();
            remove_key = queue.is_empty();
            task
        } else {
            None
        };

        if remove_key {
            pending_shadow_tasks.remove(&key);
        }

        if let Some(task) = next_task {
            inflight_shadow_keys.insert(key);
            *pending_shadow_task_count = pending_shadow_task_count.saturating_sub(1);
            return Some(task);
        }
    }
    None
}

fn rebuild_shadow_ready_queue(
    pending_shadow_tasks: &HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &HashSet<ShadowTaskKey>,
) {
    ready_shadow_keys.clear();
    ready_shadow_key_set.clear();
    for (key, pending) in pending_shadow_tasks {
        if pending.is_empty() || inflight_shadow_keys.contains(key) {
            continue;
        }
        if ready_shadow_key_set.insert(key.clone()) {
            ready_shadow_keys.push_back(key.clone());
        }
    }
}

fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }
    "unknown panic payload".to_string()
}

fn mark_shadow_task_complete(
    key: &ShadowTaskKey,
    pending_shadow_tasks: &HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    ready_shadow_keys: &mut VecDeque<ShadowTaskKey>,
    ready_shadow_key_set: &mut HashSet<ShadowTaskKey>,
    inflight_shadow_keys: &mut HashSet<ShadowTaskKey>,
) {
    inflight_shadow_keys.remove(key);
    if pending_shadow_tasks
        .get(key)
        .is_some_and(|pending| !pending.is_empty())
        && ready_shadow_key_set.insert(key.clone())
    {
        ready_shadow_keys.push_back(key.clone());
    }
}

fn handle_shadow_task_output(
    task_output: ShadowTaskOutput,
    open_shadow_lots: &mut HashSet<(String, String)>,
    shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
    shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
) {
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
            let key = (result.wallet_id, result.token);
            if result.side == "buy" {
                open_shadow_lots.insert(key);
            } else if result.side == "sell" {
                if result.has_open_lots_after_signal.unwrap_or(false) {
                    open_shadow_lots.insert(key);
                } else {
                    open_shadow_lots.remove(&key);
                }
            }
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

fn shadow_task(
    shadow: ShadowService,
    sqlite_path: &str,
    task_input: ShadowTaskInput,
) -> ShadowTaskOutput {
    let ShadowTaskInput {
        swap,
        follow_snapshot,
        key,
    } = task_input;
    let signature = swap.signature.clone();
    let outcome = (|| -> Result<ShadowProcessOutcome> {
        let store = SqliteStore::open(Path::new(sqlite_path)).with_context(|| {
            format!("failed to open sqlite db for shadow worker task: {sqlite_path}")
        })?;
        shadow.process_swap(&store, &swap, follow_snapshot.as_ref(), Utc::now())
    })();
    ShadowTaskOutput {
        signature,
        key,
        outcome,
    }
}

#[cfg(test)]
mod app_tests {
    use super::*;

    #[test]
    fn scheduler_keeps_single_inflight_per_wallet_token_key() {
        fn make_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: token.to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut pending_shadow_tasks: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> =
            HashMap::new();
        let mut pending_shadow_task_count = 0usize;
        let mut ready_shadow_keys: VecDeque<ShadowTaskKey> = VecDeque::new();
        let mut ready_shadow_key_set: HashSet<ShadowTaskKey> = HashSet::new();
        let mut inflight_shadow_keys: HashSet<ShadowTaskKey> = HashSet::new();

        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            SHADOW_PENDING_TASK_CAPACITY,
            make_task("A1", "wallet-a", "token-x"),
        )
        .is_ok());
        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            SHADOW_PENDING_TASK_CAPACITY,
            make_task("A2", "wallet-a", "token-x"),
        )
        .is_ok());
        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            SHADOW_PENDING_TASK_CAPACITY,
            make_task("B1", "wallet-b", "token-y"),
        )
        .is_ok());
        assert_eq!(pending_shadow_task_count, 3);

        let first = dequeue_next_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &mut inflight_shadow_keys,
        )
        .expect("first task");
        let second = dequeue_next_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &mut inflight_shadow_keys,
        )
        .expect("second task");

        assert_eq!(first.swap.signature, "A1");
        assert_eq!(second.swap.signature, "B1");

        mark_shadow_task_complete(
            &first.key,
            &pending_shadow_tasks,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &mut inflight_shadow_keys,
        );

        let third = dequeue_next_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &mut inflight_shadow_keys,
        )
        .expect("third task");

        assert_eq!(third.swap.signature, "A2");
        assert_eq!(pending_shadow_task_count, 0);
    }

    #[test]
    fn enqueue_shadow_task_enforces_capacity() {
        fn make_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: token.to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut pending_shadow_tasks: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> =
            HashMap::new();
        let mut pending_shadow_task_count = 0usize;
        let mut ready_shadow_keys: VecDeque<ShadowTaskKey> = VecDeque::new();
        let mut ready_shadow_key_set: HashSet<ShadowTaskKey> = HashSet::new();
        let inflight_shadow_keys: HashSet<ShadowTaskKey> = HashSet::new();
        let cap = 2usize;

        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            make_task("A1", "wallet-a", "token-x"),
        )
        .is_ok());
        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            make_task("A2", "wallet-a", "token-x"),
        )
        .is_ok());
        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            make_task("B1", "wallet-b", "token-y"),
        )
        .is_err());
        assert_eq!(pending_shadow_task_count, cap);
    }

    #[test]
    fn queue_full_prefers_sell_over_buy_under_mixed_flow() {
        fn make_buy_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: token.to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        fn make_sell_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: token.to_string(),
                    token_out: "So11111111111111111111111111111111111111112".to_string(),
                    amount_in: 1000.0,
                    amount_out: 1.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut pending_shadow_tasks: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> =
            HashMap::new();
        let mut pending_shadow_task_count = 0usize;
        let mut ready_shadow_keys: VecDeque<ShadowTaskKey> = VecDeque::new();
        let mut ready_shadow_key_set: HashSet<ShadowTaskKey> = HashSet::new();
        let mut inflight_shadow_keys: HashSet<ShadowTaskKey> = HashSet::new();
        let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let cap = 1usize;

        assert!(enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            make_buy_task("B0", "wallet-buy", "token-buy"),
        )
        .is_ok());
        assert_eq!(pending_shadow_task_count, 1);

        let overflow_buy = enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            make_buy_task("B1", "wallet-buy-2", "token-buy-2"),
        )
        .expect_err("buy should overflow at cap");
        handle_shadow_enqueue_overflow(
            ShadowSwapSide::Buy,
            overflow_buy,
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
        );

        assert_eq!(pending_shadow_task_count, 1);
        assert_eq!(
            shadow_queue_full_outcome_counts.get("queue_full_buy_drop"),
            Some(&1)
        );

        let sell_task = make_sell_task("S1", "wallet-sell", "token-sell");
        let sell_key = sell_task.key.clone();
        inflight_shadow_keys.insert(sell_key.clone());
        let overflow_sell = enqueue_shadow_task(
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            sell_task,
        )
        .expect_err("sell should overflow at cap before policy handling");

        handle_shadow_enqueue_overflow(
            ShadowSwapSide::Sell,
            overflow_sell,
            &mut pending_shadow_tasks,
            &mut pending_shadow_task_count,
            &mut ready_shadow_keys,
            &mut ready_shadow_key_set,
            &inflight_shadow_keys,
            cap,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
        );

        assert_eq!(pending_shadow_task_count, 1);
        assert_eq!(
            shadow_queue_full_outcome_counts.get("queue_full_sell_kept"),
            Some(&1)
        );
        assert!(
            !shadow_queue_full_outcome_counts.contains_key("queue_full_sell_dropped"),
            "sell should be kept when a pending buy can be evicted"
        );

        let queued_sell = pending_shadow_tasks
            .get(&sell_key)
            .expect("sell task should remain queued");
        assert_eq!(queued_sell.len(), 1);
        assert!(
            ready_shadow_key_set.get(&sell_key).is_none(),
            "inflight key must not be marked ready"
        );
    }

    #[test]
    fn inline_processing_respects_per_key_serialization() {
        let key = ShadowTaskKey {
            wallet: "wallet-a".to_string(),
            token: "token-x".to_string(),
        };
        let mut pending_shadow_tasks: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>> =
            HashMap::new();
        let mut inflight_shadow_keys: HashSet<ShadowTaskKey> = HashSet::new();

        assert!(should_process_shadow_inline(
            true,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
            &pending_shadow_tasks,
            &inflight_shadow_keys,
        ));

        inflight_shadow_keys.insert(key.clone());
        assert!(!should_process_shadow_inline(
            true,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
            &pending_shadow_tasks,
            &inflight_shadow_keys,
        ));

        inflight_shadow_keys.clear();
        pending_shadow_tasks.insert(
            key.clone(),
            VecDeque::from([ShadowTaskInput {
                swap: SwapEvent {
                    wallet: "wallet-a".to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: "token-x".to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: "sig-queued".to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: key.clone(),
            }]),
        );
        assert!(!should_process_shadow_inline(
            true,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
            &pending_shadow_tasks,
            &inflight_shadow_keys,
        ));
        assert!(!should_process_shadow_inline(
            true,
            true,
            SHADOW_WORKER_POOL_SIZE,
            &key,
            &pending_shadow_tasks,
            &inflight_shadow_keys,
        ));
        assert!(!should_process_shadow_inline(
            false,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
            &pending_shadow_tasks,
            &inflight_shadow_keys,
        ));
        assert!(!should_process_shadow_inline(
            true,
            false,
            SHADOW_MAX_CONCURRENT_WORKERS,
            &key,
            &pending_shadow_tasks,
            &inflight_shadow_keys,
        ));
    }

    #[test]
    fn follow_snapshot_uses_temporal_promotions_and_demotions() {
        let mut snapshot = FollowSnapshot::default();
        let promoted = DateTime::parse_from_rfc3339("2026-02-15T10:00:00Z")
            .expect("rfc3339")
            .with_timezone(&Utc);
        let demoted = DateTime::parse_from_rfc3339("2026-02-15T11:00:00Z")
            .expect("rfc3339")
            .with_timezone(&Utc);
        snapshot.promoted_at.insert("w".to_string(), promoted);
        snapshot.demoted_at.insert("w".to_string(), demoted);

        assert!(snapshot.is_followed_at("w", promoted + chrono::Duration::minutes(10)));
        assert!(!snapshot.is_followed_at("w", demoted + chrono::Duration::seconds(1)));
    }
}
