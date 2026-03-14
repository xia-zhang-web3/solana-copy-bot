use anyhow::{anyhow, Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage::{DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, SqliteStore};
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

const OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY: usize = 4096;
const OBSERVED_SWAP_BATCH_MAX_SIZE: usize = 128;
pub(crate) const OBSERVED_SWAP_RETENTION_SWEEP_INTERVAL: StdDuration =
    StdDuration::from_secs(15 * 60);
const OBSERVED_SWAP_WRITER_LATENCY_SAMPLE_CAPACITY: usize = 512;
pub(crate) const OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT: &str =
    "observed swap writer channel closed";
pub(crate) const OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT: &str =
    "observed swap writer reply channel closed";
pub(crate) const OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT: &str =
    "observed swap writer terminal failure";

#[derive(Clone)]
struct ObservedSwapWriterConfig {
    channel_capacity: usize,
    batch_max_size: usize,
    aggregate_writes_enabled: bool,
    aggregate_write_config: DiscoveryAggregateWriteConfig,
}

impl ObservedSwapWriterConfig {
    fn production(
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
    ) -> Self {
        Self {
            channel_capacity: OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            batch_max_size: OBSERVED_SWAP_BATCH_MAX_SIZE,
            aggregate_writes_enabled,
            aggregate_write_config,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ObservedSwapRetentionConfig {
    pub retention_days: u32,
    pub aggregate_retention_days: u32,
    pub aggregate_writes_enabled: bool,
}

impl ObservedSwapRetentionConfig {
    pub(crate) fn production(
        retention_days: u32,
        aggregate_retention_days: u32,
        aggregate_writes_enabled: bool,
    ) -> Self {
        Self {
            retention_days,
            aggregate_retention_days,
            aggregate_writes_enabled,
        }
    }
}

struct ObservedSwapWriteRequest {
    swap: SwapEvent,
    reply_tx: Option<oneshot::Sender<Result<bool>>>,
    enqueued_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservedSwapWriterSnapshot {
    pub pending_requests: usize,
    pub write_latency_ms_p95: u64,
}

#[derive(Debug, Default)]
struct ObservedSwapWriterTelemetry {
    pending_requests: AtomicUsize,
    last_write_latency_ms_p95: AtomicU64,
    write_latency_ms_samples: Mutex<VecDeque<u64>>,
}

impl ObservedSwapWriterTelemetry {
    fn note_enqueued(&self) {
        self.pending_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn note_batch_completed(&self, queued_at: &[Instant]) {
        if queued_at.is_empty() {
            return;
        }
        let now = Instant::now();
        if let Ok(mut samples) = self.write_latency_ms_samples.lock() {
            for queued_at in queued_at {
                let latency_ms = now
                    .duration_since(*queued_at)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64;
                if samples.len() >= OBSERVED_SWAP_WRITER_LATENCY_SAMPLE_CAPACITY {
                    let _ = samples.pop_front();
                }
                samples.push_back(latency_ms);
            }
            self.last_write_latency_ms_p95
                .store(percentile_from_deque(&samples, 0.95), Ordering::Relaxed);
        }
        let _ =
            self.pending_requests
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(queued_at.len()))
                });
    }

    fn snapshot(&self) -> ObservedSwapWriterSnapshot {
        let write_latency_ms_p95 = self
            .write_latency_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| self.last_write_latency_ms_p95.load(Ordering::Relaxed));
        ObservedSwapWriterSnapshot {
            pending_requests: self.pending_requests.load(Ordering::Relaxed),
            write_latency_ms_p95,
        }
    }
}

pub(crate) struct ObservedSwapWriter {
    sender: mpsc::Sender<ObservedSwapWriteRequest>,
    worker: Option<thread::JoinHandle<Result<()>>>,
    telemetry: Arc<ObservedSwapWriterTelemetry>,
    terminal_failure_message: Arc<Mutex<Option<String>>>,
}

impl ObservedSwapWriter {
    fn terminal_failure_error(&self) -> Option<anyhow::Error> {
        self.terminal_failure_message
            .lock()
            .ok()
            .and_then(|message| message.clone())
            .map(|message| anyhow!(message).context(OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT))
    }

    pub(crate) fn ensure_running(&self) -> Result<()> {
        if let Some(error) = self.terminal_failure_error() {
            return Err(error);
        }
        Ok(())
    }

    async fn send_request(&self, request: ObservedSwapWriteRequest) -> Result<()> {
        self.ensure_running()?;
        let permit = self
            .sender
            .reserve()
            .await
            .context(OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT)?;
        self.telemetry.note_enqueued();
        permit.send(request);
        Ok(())
    }

    pub(crate) fn start(
        sqlite_path: String,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
    ) -> Result<Self> {
        Self::start_with_config(
            sqlite_path,
            ObservedSwapWriterConfig::production(aggregate_writes_enabled, aggregate_write_config),
        )
    }

    fn start_with_config(sqlite_path: String, config: ObservedSwapWriterConfig) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(config.channel_capacity);
        let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
        let terminal_failure_message = Arc::new(Mutex::new(None));
        let worker_telemetry = Arc::clone(&telemetry);
        let worker_terminal_failure_message = Arc::clone(&terminal_failure_message);
        let worker = thread::Builder::new()
            .name("copybot-observed-swap-writer".to_string())
            .spawn(move || {
                let result =
                    observed_swap_writer_loop(sqlite_path, receiver, config, worker_telemetry);
                if let Err(error) = &result {
                    if let Ok(mut message) = worker_terminal_failure_message.lock() {
                        *message = Some(format!("{error:#}"));
                    }
                }
                result
            })
            .context("failed to spawn observed swap writer thread")?;
        Ok(Self {
            sender,
            worker: Some(worker),
            telemetry,
            terminal_failure_message,
        })
    }

    pub(crate) async fn enqueue(&self, swap: &SwapEvent) -> Result<()> {
        self.send_request(ObservedSwapWriteRequest {
            swap: swap.clone(),
            reply_tx: None,
            enqueued_at: Instant::now(),
        })
        .await
    }

    pub(crate) fn try_enqueue(&self, swap: &SwapEvent) -> Result<bool> {
        self.ensure_running()?;
        match self.sender.try_reserve() {
            Ok(permit) => {
                self.telemetry.note_enqueued();
                permit.send(ObservedSwapWriteRequest {
                    swap: swap.clone(),
                    reply_tx: None,
                    enqueued_at: Instant::now(),
                });
                Ok(true)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow!(OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT))
                    .context(OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT)
            }
        }
    }

    pub(crate) async fn write(&self, swap: &SwapEvent) -> Result<bool> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send_request(ObservedSwapWriteRequest {
            swap: swap.clone(),
            reply_tx: Some(reply_tx),
            enqueued_at: Instant::now(),
        })
        .await?;
        reply_rx
            .await
            .context(OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT)?
    }

    pub(crate) fn snapshot(&self) -> ObservedSwapWriterSnapshot {
        self.telemetry.snapshot()
    }

    pub(crate) fn shutdown(mut self) -> Result<()> {
        drop(self.sender);
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker
            .join()
            .map_err(|payload| anyhow!(panic_payload_to_string(payload.as_ref())))
            .context("observed swap writer thread panicked")?
            .context("observed swap writer thread failed")
    }
}

fn observed_swap_writer_loop(
    sqlite_path: String,
    mut receiver: mpsc::Receiver<ObservedSwapWriteRequest>,
    config: ObservedSwapWriterConfig,
    telemetry: Arc<ObservedSwapWriterTelemetry>,
) -> Result<()> {
    let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
        format!("failed to open sqlite db for observed swap writer: {sqlite_path}")
    })?;
    run_aggregate_startup_replay(&store, &config)?;

    while let Some(first_request) = receiver.blocking_recv() {
        let mut batch = vec![first_request];
        while batch.len() < config.batch_max_size {
            match receiver.try_recv() {
                Ok(request) => batch.push(request),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        let mut swaps = Vec::with_capacity(batch.len());
        let mut replies = Vec::with_capacity(batch.len());
        let mut queued_at = Vec::with_capacity(batch.len());
        for request in batch {
            swaps.push(request.swap);
            replies.push(request.reply_tx);
            queued_at.push(request.enqueued_at);
        }

        match store.insert_observed_swaps_batch_with_activity_days(&swaps) {
            Ok(results) => {
                for (reply_tx, inserted) in replies.into_iter().zip(results.iter().copied()) {
                    if let Some(reply_tx) = reply_tx {
                        let _ = reply_tx.send(Ok(inserted));
                    }
                }
                telemetry.note_batch_completed(&queued_at);
                if config.aggregate_writes_enabled {
                    let inserted_swaps: Vec<SwapEvent> = swaps
                        .iter()
                        .zip(results.iter())
                        .filter_map(|(swap, inserted)| inserted.then_some(swap.clone()))
                        .collect();
                    if !inserted_swaps.is_empty() {
                        let aggregate_result = store.apply_discovery_scoring_batch(
                            &inserted_swaps,
                            &config.aggregate_write_config,
                        );
                        if let Err(error) = aggregate_result {
                            if let Some(first_gap_swap) = inserted_swaps.iter().min_by(|a, b| {
                                a.ts_utc
                                    .cmp(&b.ts_utc)
                                    .then_with(|| a.slot.cmp(&b.slot))
                                    .then_with(|| a.signature.cmp(&b.signature))
                            }) {
                                if let Err(gap_error) = store
                                    .set_discovery_scoring_materialization_gap_cursor(
                                        &DiscoveryRuntimeCursor {
                                            ts_utc: first_gap_swap.ts_utc,
                                            slot: first_gap_swap.slot,
                                            signature: first_gap_swap.signature.clone(),
                                        },
                                    )
                                {
                                    warn!(
                                        error = %gap_error,
                                        gap_since = %first_gap_swap.ts_utc,
                                        "failed to latch discovery scoring materialization gap after aggregate batch failure"
                                    );
                                }
                            }
                            warn!(
                                error = %error,
                                inserted_swaps = inserted_swaps.len(),
                                "observed swap batch inserted raw rows but discovery scoring materialization failed"
                            );
                        } else if let Some(max_swap) = inserted_swaps.iter().max_by(|a, b| {
                            a.ts_utc
                                .cmp(&b.ts_utc)
                                .then_with(|| a.slot.cmp(&b.slot))
                                .then_with(|| a.signature.cmp(&b.signature))
                        }) {
                            if let Err(error) = store.set_discovery_scoring_covered_through_cursor(
                                &DiscoveryRuntimeCursor {
                                    ts_utc: max_swap.ts_utc,
                                    slot: max_swap.slot,
                                    signature: max_swap.signature.clone(),
                                },
                            ) {
                                warn!(
                                    error = %error,
                                    covered_through = %max_swap.ts_utc,
                                    "observed swap batch materialized discovery scoring aggregates but failed to advance coverage watermark"
                                );
                            }
                        }
                    }
                }
            }
            Err(error) => {
                let message = format!("{error:#}");
                warn!(
                    error = %error,
                    batch_swaps = swaps.len(),
                    "failed to insert observed swap batch with activity days"
                );
                for reply_tx in replies {
                    if let Some(reply_tx) = reply_tx {
                        let _ = reply_tx.send(Err(anyhow!(message.clone())));
                    }
                }
                telemetry.note_batch_completed(&queued_at);
                return Err(anyhow!(message))
                    .context("observed swap writer stopping after raw batch insert failure");
            }
        }
    }

    Ok(())
}

pub(crate) fn run_observed_swap_retention_maintenance_once(
    sqlite_path: &str,
    config: ObservedSwapRetentionConfig,
) -> Result<()> {
    let store = SqliteStore::open(Path::new(sqlite_path)).with_context(|| {
        format!("failed to open sqlite db for observed swap retention maintenance: {sqlite_path}")
    })?;
    run_observed_swap_retention_maintenance(&store, config);
    Ok(())
}

fn run_observed_swap_retention_maintenance(
    store: &SqliteStore,
    config: ObservedSwapRetentionConfig,
) {
    let now = Utc::now();
    let nominal_cutoff = now - ChronoDuration::days(config.retention_days.max(1) as i64);
    let effective_cutoff = match store.load_discovery_scoring_backfill_protected_since(now) {
        Ok(Some(protected_since)) => nominal_cutoff.min(protected_since),
        Ok(None) => nominal_cutoff,
        Err(error) => {
            warn!(
                error = %error,
                retention_days = config.retention_days,
                "failed loading discovery scoring backfill source protection; using nominal observed swap retention cutoff"
            );
            nominal_cutoff
        }
    };
    match store.delete_observed_swaps_before(effective_cutoff) {
        Ok(deleted_raw) => {
            let deleted_scoring = if config.aggregate_writes_enabled {
                let aggregate_cutoff =
                    now - ChronoDuration::days(config.aggregate_retention_days.max(1) as i64);
                match store.prune_discovery_scoring_before(aggregate_cutoff) {
                    Ok(deleted_scoring) => deleted_scoring,
                    Err(error) => {
                        warn!(
                            error = %error,
                            aggregate_retention_days = config.aggregate_retention_days,
                            "discovery scoring retention sweep failed"
                        );
                        0
                    }
                }
            } else {
                0
            };
            run_retention_wal_checkpoint(
                store,
                config,
                nominal_cutoff,
                effective_cutoff,
                deleted_raw,
                deleted_scoring,
            );
        }
        Err(error) => {
            warn!(
                error = %error,
                retention_days = config.retention_days,
                nominal_observed_swap_cutoff = %nominal_cutoff,
                effective_observed_swap_cutoff = %effective_cutoff,
                "observed swap retention sweep failed"
            );
        }
    }
}

fn percentile_from_deque(values: &VecDeque<u64>, q: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let idx = ((sorted.len() - 1) as f64 * q.clamp(0.0, 1.0)).round() as usize;
    sorted[idx]
}

fn run_retention_wal_checkpoint(
    store: &SqliteStore,
    config: ObservedSwapRetentionConfig,
    nominal_cutoff: chrono::DateTime<Utc>,
    effective_cutoff: chrono::DateTime<Utc>,
    deleted_raw: usize,
    deleted_scoring: usize,
) {
    if deleted_raw > 0 || deleted_scoring > 0 {
        match store.checkpoint_wal_truncate() {
            Ok((busy, log_frames, checkpointed_frames)) if busy == 0 => {
                info!(
                    retention_days = config.retention_days,
                    aggregate_retention_days = config.aggregate_retention_days,
                    nominal_observed_swap_cutoff = %nominal_cutoff,
                    effective_observed_swap_cutoff = %effective_cutoff,
                    deleted_observed_swap_rows = deleted_raw,
                    deleted_scoring_rows = deleted_scoring,
                    wal_checkpoint_mode = "truncate",
                    wal_checkpoint_busy = busy,
                    wal_log_frames = log_frames,
                    wal_checkpointed_frames = checkpointed_frames,
                    "observed swap retention sweep reclaimed sqlite wal"
                );
                return;
            }
            Ok((busy, log_frames, checkpointed_frames)) => match store.checkpoint_wal_passive() {
                Ok((passive_busy, passive_log_frames, passive_checkpointed_frames)) => {
                    warn!(
                        retention_days = config.retention_days,
                        aggregate_retention_days = config.aggregate_retention_days,
                        nominal_observed_swap_cutoff = %nominal_cutoff,
                        effective_observed_swap_cutoff = %effective_cutoff,
                        deleted_observed_swap_rows = deleted_raw,
                        deleted_scoring_rows = deleted_scoring,
                        wal_checkpoint_mode = "truncate_then_passive",
                        wal_checkpoint_busy = busy,
                        wal_log_frames = log_frames,
                        wal_checkpointed_frames = checkpointed_frames,
                        wal_passive_checkpoint_busy = passive_busy,
                        wal_passive_log_frames = passive_log_frames,
                        wal_passive_checkpointed_frames = passive_checkpointed_frames,
                        "observed swap retention sweep truncate checkpoint was blocked by readers; passive checkpoint attempted"
                    );
                    return;
                }
                Err(passive_error) => {
                    warn!(
                        error = %passive_error,
                        retention_days = config.retention_days,
                        aggregate_retention_days = config.aggregate_retention_days,
                        nominal_observed_swap_cutoff = %nominal_cutoff,
                        effective_observed_swap_cutoff = %effective_cutoff,
                        deleted_observed_swap_rows = deleted_raw,
                        deleted_scoring_rows = deleted_scoring,
                        wal_checkpoint_mode = "truncate_then_passive",
                        wal_checkpoint_busy = busy,
                        wal_log_frames = log_frames,
                        wal_checkpointed_frames = checkpointed_frames,
                        "observed swap retention sweep truncate checkpoint was blocked by readers and passive checkpoint failed"
                    );
                    return;
                }
            },
            Err(error) => match store.checkpoint_wal_passive() {
                Ok((passive_busy, passive_log_frames, passive_checkpointed_frames)) => {
                    warn!(
                        error = %error,
                        retention_days = config.retention_days,
                        aggregate_retention_days = config.aggregate_retention_days,
                        nominal_observed_swap_cutoff = %nominal_cutoff,
                        effective_observed_swap_cutoff = %effective_cutoff,
                        deleted_observed_swap_rows = deleted_raw,
                        deleted_scoring_rows = deleted_scoring,
                        wal_checkpoint_mode = "passive_fallback",
                        wal_passive_checkpoint_busy = passive_busy,
                        wal_passive_log_frames = passive_log_frames,
                        wal_passive_checkpointed_frames = passive_checkpointed_frames,
                        "observed swap retention sweep truncate checkpoint failed; passive checkpoint attempted"
                    );
                    return;
                }
                Err(passive_error) => {
                    warn!(
                        error = %error,
                        fallback_error = %passive_error,
                        retention_days = config.retention_days,
                        aggregate_retention_days = config.aggregate_retention_days,
                        nominal_observed_swap_cutoff = %nominal_cutoff,
                        effective_observed_swap_cutoff = %effective_cutoff,
                        deleted_observed_swap_rows = deleted_raw,
                        deleted_scoring_rows = deleted_scoring,
                        "observed swap retention sweep deleted rows but wal checkpoints failed"
                    );
                    return;
                }
            },
        }
    }

    match store.checkpoint_wal_passive() {
        Ok((passive_busy, passive_log_frames, passive_checkpointed_frames)) => {
            info!(
                retention_days = config.retention_days,
                aggregate_retention_days = config.aggregate_retention_days,
                nominal_observed_swap_cutoff = %nominal_cutoff,
                effective_observed_swap_cutoff = %effective_cutoff,
                deleted_observed_swap_rows = deleted_raw,
                deleted_scoring_rows = deleted_scoring,
                wal_checkpoint_mode = "passive_periodic",
                wal_passive_checkpoint_busy = passive_busy,
                wal_passive_log_frames = passive_log_frames,
                wal_passive_checkpointed_frames = passive_checkpointed_frames,
                "observed swap retention sweep attempted periodic passive wal checkpoint"
            );
        }
        Err(error) => {
            warn!(
                error = %error,
                retention_days = config.retention_days,
                aggregate_retention_days = config.aggregate_retention_days,
                nominal_observed_swap_cutoff = %nominal_cutoff,
                effective_observed_swap_cutoff = %effective_cutoff,
                deleted_observed_swap_rows = deleted_raw,
                deleted_scoring_rows = deleted_scoring,
                wal_checkpoint_mode = "passive_periodic",
                "observed swap retention sweep periodic passive wal checkpoint failed"
            );
        }
    }
}

fn run_aggregate_startup_replay(
    store: &SqliteStore,
    config: &ObservedSwapWriterConfig,
) -> Result<()> {
    if !config.aggregate_writes_enabled {
        return Ok(());
    }

    let covered_since = store.load_discovery_scoring_covered_since()?;
    let mut cursor = match store.load_discovery_scoring_covered_through_cursor()? {
        Some(cursor) => cursor,
        None => {
            if covered_since.is_some() {
                return Err(anyhow!(
                    "aggregate writes require an exact covered_through cursor for safe startup replay"
                ));
            }
            return Ok(());
        }
    };
    let gap_cursor = store.load_discovery_scoring_materialization_gap_cursor()?;
    let mut gap_cursor_observed = false;

    loop {
        let mut page = Vec::with_capacity(config.batch_max_size);
        let rows_seen = store.for_each_observed_swap_after_cursor(
            cursor.ts_utc,
            cursor.slot,
            cursor.signature.as_str(),
            config.batch_max_size,
            |swap| {
                if gap_cursor.as_ref().is_some_and(|gap_cursor| {
                    gap_cursor.ts_utc == swap.ts_utc
                        && gap_cursor.slot == swap.slot
                        && gap_cursor.signature == swap.signature
                }) {
                    gap_cursor_observed = true;
                }
                page.push(swap);
                Ok(())
            },
        )?;
        if page.is_empty() {
            break;
        }

        if let Err(error) =
            store.apply_discovery_scoring_batch(&page, &config.aggregate_write_config)
        {
            if let Some(first_gap_swap) = page.iter().min_by(|a, b| {
                a.ts_utc
                    .cmp(&b.ts_utc)
                    .then_with(|| a.slot.cmp(&b.slot))
                    .then_with(|| a.signature.cmp(&b.signature))
            }) {
                let _ = store.set_discovery_scoring_materialization_gap_cursor(
                    &DiscoveryRuntimeCursor {
                        ts_utc: first_gap_swap.ts_utc,
                        slot: first_gap_swap.slot,
                        signature: first_gap_swap.signature.clone(),
                    },
                );
            }
            return Err(error).context(
                "failed replaying discovery scoring rows during aggregate-writer startup catch-up",
            );
        }

        let last_swap = page
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("aggregate startup replay page unexpectedly empty"))?;
        store.finalize_discovery_scoring_rug_facts(last_swap.ts_utc)?;
        cursor = DiscoveryRuntimeCursor {
            ts_utc: last_swap.ts_utc,
            slot: last_swap.slot,
            signature: last_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&cursor)?;

        if rows_seen < config.batch_max_size {
            break;
        }
    }

    if gap_cursor_observed {
        if let Some(gap_cursor) = gap_cursor.as_ref() {
            store.clear_discovery_scoring_materialization_gap_if_cursor_observed(gap_cursor)?;
        }
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::{ObservedSwapWriter, ObservedSwapWriterConfig};
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use copybot_core_types::SwapEvent;
    use copybot_storage::{DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, SqliteStore};
    use rusqlite::Connection;
    use std::path::Path;
    use std::thread;
    use std::time::Duration as StdDuration;
    use tokio::runtime::Builder;
    use tokio::time::{sleep, timeout, Duration};

    fn aggregate_write_config() -> DiscoveryAggregateWriteConfig {
        DiscoveryAggregateWriteConfig::default()
    }

    #[test]
    fn observed_swap_writer_does_not_block_runtime_under_sqlite_lock() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-writer-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let swap = SwapEvent {
            wallet: "wallet-async".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-async".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-async".to_string(),
            slot: 123,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        let runtime_handle = thread::spawn(move || -> Result<bool> {
            let runtime = Builder::new_current_thread().enable_all().build()?;
            runtime.block_on(async move {
                let writer =
                    ObservedSwapWriter::start(sqlite_path.clone(), true, aggregate_write_config())?;
                let swap_for_task = swap.clone();
                let insert_task = tokio::spawn(async move { writer.write(&swap_for_task).await });

                timeout(Duration::from_millis(50), sleep(Duration::from_millis(10)))
                    .await
                    .context(
                        "current-thread runtime stalled while observed swap writer was blocked",
                    )?;

                insert_task
                    .await
                    .context("observed swap task join failed")?
            })
        });

        std::thread::sleep(StdDuration::from_millis(250));
        blocker_conn.execute_batch("COMMIT")?;

        let inserted = runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context("observed swap write should succeed after lock release")?;
        assert!(inserted, "observed swap insert should report a fresh write");

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-async");
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_enqueue_returns_before_locked_batch_commits() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-enqueue-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let swap = SwapEvent {
            wallet: "wallet-enqueue".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-enqueue".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-enqueue".to_string(),
            slot: 124,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async move {
            let writer = ObservedSwapWriter::start(sqlite_path, true, aggregate_write_config())?;
            timeout(Duration::from_millis(50), writer.enqueue(&swap))
                .await
                .context("observed swap enqueue should not wait for batch commit")??;
            Ok::<ObservedSwapWriter, anyhow::Error>(writer)
        })?;
        let pending_snapshot = writer.snapshot();
        assert_eq!(
            pending_snapshot.pending_requests, 1,
            "snapshot should expose the locked batch as one pending observed swap write"
        );

        let verify_before_commit = SqliteStore::open(Path::new(&db_path))?;
        let before_swaps = verify_before_commit.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert!(
            before_swaps.is_empty(),
            "enqueue should not imply the batch has already committed under sqlite lock"
        );

        std::thread::sleep(StdDuration::from_millis(50));
        blocker_conn.execute_batch("COMMIT")?;
        std::thread::sleep(StdDuration::from_millis(50));
        let committed_snapshot = writer.snapshot();
        assert_eq!(
            committed_snapshot.pending_requests, 0,
            "snapshot should clear pending depth after the blocked batch commits"
        );
        assert!(
            committed_snapshot.write_latency_ms_p95 >= 40,
            "snapshot should retain queue+commit latency once the blocked batch completes"
        );
        writer.shutdown()?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-enqueue");
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_try_enqueue_returns_false_when_channel_is_full() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-try-enqueue-full-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig {
                channel_capacity: 1,
                batch_max_size: 1,
                aggregate_writes_enabled: true,
                aggregate_write_config: aggregate_write_config(),
            },
        )?;

        let first_swap = SwapEvent {
            wallet: "wallet-try-enqueue".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-try-enqueue-a".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-try-enqueue-a".to_string(),
            slot: 125,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:10:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        assert!(writer.try_enqueue(&first_swap)?);
        let mut saw_full = false;
        for idx in 0..32u64 {
            let swap = SwapEvent {
                token_out: format!("token-try-enqueue-{idx}"),
                signature: format!("sig-try-enqueue-{idx}"),
                slot: 126 + idx,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:10:01Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                ..first_swap.clone()
            };
            if !writer.try_enqueue(&swap)? {
                saw_full = true;
                break;
            }
        }
        assert!(
            saw_full,
            "non-blocking try_enqueue should report a full channel instead of waiting once the bounded queue saturates"
        );

        blocker_conn.execute_batch("COMMIT")?;
        std::thread::sleep(StdDuration::from_millis(50));
        writer.shutdown()?;
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_snapshot_clears_pending_after_fast_successful_enqueue() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-fast-snapshot-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                true,
                aggregate_write_config(),
            )
        })?;

        for idx in 0..8 {
            let swap = SwapEvent {
                wallet: "wallet-fast-snapshot".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: format!("token-fast-snapshot-{idx}"),
                amount_in: 1.0,
                amount_out: 10.0 + idx as f64,
                signature: format!("sig-observed-swap-fast-snapshot-{idx}"),
                slot: 300 + idx as u64,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-14T14:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };
            runtime.block_on(async { writer.enqueue(&swap).await })?;
        }

        std::thread::sleep(StdDuration::from_millis(50));
        let snapshot = writer.snapshot();
        assert_eq!(
            snapshot.pending_requests, 0,
            "fast successful batches must not leave phantom pending writer requests"
        );
        assert!(
            snapshot.write_latency_ms_p95 < 250,
            "fast successful batches should not report lock-scale writer latency"
        );

        writer.shutdown()?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T13:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 8);
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_closes_channel_after_async_batch_insert_failure() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-async-fail-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for async failure trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_activity_days_insert
             BEFORE INSERT ON wallet_activity_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced async observed swap failure');
             END;",
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig {
                    channel_capacity: 16,
                    batch_max_size: 8,
                    aggregate_writes_enabled: true,
                    aggregate_write_config: aggregate_write_config(),
                },
            )
        })?;

        let failing_swap = SwapEvent {
            wallet: "wallet-async-fail".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-async-fail".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-async-fail".to_string(),
            slot: 200,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let subsequent_swap = SwapEvent {
            signature: "sig-observed-swap-after-fail".to_string(),
            slot: 201,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:01:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            ..failing_swap.clone()
        };

        runtime.block_on(async { writer.enqueue(&failing_swap).await })?;
        std::thread::sleep(StdDuration::from_millis(50));

        let error = runtime
            .block_on(async { writer.enqueue(&subsequent_swap).await })
            .expect_err("writer channel should close after async raw batch insert failure");
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT)
                || error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected enqueue-after-failure error: {error_chain}"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface async raw batch insert failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("forced async observed swap failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T12:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert!(
            swaps.is_empty(),
            "failed async batch insert must not leave partially persisted observed swaps"
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_reports_terminal_failure_after_async_batch_insert_failure() -> Result<()>
    {
        let unique = format!(
            "copybot-app-observed-swap-async-health-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for async failure trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_activity_days_insert
             BEFORE INSERT ON wallet_activity_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced async observed swap failure');
             END;",
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig {
                    channel_capacity: 16,
                    batch_max_size: 8,
                    aggregate_writes_enabled: true,
                    aggregate_write_config: aggregate_write_config(),
                },
            )
        })?;

        let failing_swap = SwapEvent {
            wallet: "wallet-async-health".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-async-health".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-async-health".to_string(),
            slot: 210,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:10:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        runtime.block_on(async { writer.enqueue(&failing_swap).await })?;
        std::thread::sleep(StdDuration::from_millis(50));

        let error = writer
            .ensure_running()
            .expect_err("terminal async writer failure should be latched before next enqueue");
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected terminal failure health-check error: {error_chain}"
        );
        assert!(
            error_chain.contains("forced async observed swap failure"),
            "unexpected terminal failure health-check chain: {error_chain}"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface async raw batch insert failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("forced async observed swap failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_keeps_retention_out_of_inline_batch_path() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-retention-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig {
                    channel_capacity: 16,
                    batch_max_size: 8,
                    aggregate_writes_enabled: true,
                    aggregate_write_config: aggregate_write_config(),
                },
            )?;

            let stale_swap = SwapEvent {
                wallet: "wallet-old".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-old".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-observed-swap-old".to_string(),
                slot: 100,
                ts_utc: Utc::now() - ChronoDuration::days(3),
                exact_amounts: None,
            };
            let fresh_swap = SwapEvent {
                wallet: "wallet-new".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-new".to_string(),
                amount_in: 2.0,
                amount_out: 20.0,
                signature: "sig-observed-swap-new".to_string(),
                slot: 101,
                ts_utc: Utc::now(),
                exact_amounts: None,
            };

            writer.write(&stale_swap).await?;
            writer.write(&fresh_swap).await?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps_before_maintenance =
            verify_store.load_observed_swaps_since(Utc::now() - ChronoDuration::days(7))?;
        assert_eq!(
            swaps_before_maintenance.len(),
            2,
            "writer should no longer prune stale rows inline while inserting fresh observed swaps"
        );

        super::run_observed_swap_retention_maintenance_once(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            super::ObservedSwapRetentionConfig::production(1, 7, true),
        )?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps_after_maintenance =
            verify_store.load_observed_swaps_since(Utc::now() - ChronoDuration::days(7))?;
        assert_eq!(swaps_after_maintenance.len(), 1);
        assert_eq!(
            swaps_after_maintenance[0].signature,
            "sig-observed-swap-new"
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_retention_maintenance_respects_backfill_source_protection() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-retention-protect-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let stale_swap = SwapEvent {
            wallet: "wallet-protected-old".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-protected-old".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-protected-old".to_string(),
            slot: 100,
            ts_utc: Utc::now() - ChronoDuration::days(3),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[stale_swap.clone()])?;
        seed_store.set_discovery_scoring_backfill_source_protection(
            Utc::now() - ChronoDuration::days(4),
            Utc::now() + ChronoDuration::hours(1),
        )?;

        super::run_observed_swap_retention_maintenance_once(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            super::ObservedSwapRetentionConfig::production(1, 7, false),
        )?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let stale_rows = verify_store
            .load_observed_swaps_since(Utc::now() - ChronoDuration::days(7))?
            .into_iter()
            .filter(|swap| swap.signature == "sig-protected-old")
            .count();
        assert_eq!(
            stale_rows, 1,
            "source protection must defer raw retention pruning"
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_startup_replay_clears_observed_materialization_gap() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-gap-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let covered_swap = SwapEvent {
            wallet: "wallet-gap".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-gap".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-gap-covered".to_string(),
            slot: 99,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[covered_swap.clone()])?;
        seed_store.apply_discovery_scoring_batch(
            std::slice::from_ref(&covered_swap),
            &aggregate_write_config(),
        )?;
        seed_store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: covered_swap.ts_utc,
            slot: covered_swap.slot,
            signature: covered_swap.signature.clone(),
        })?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for gap trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_scoring_days_insert
             BEFORE INSERT ON wallet_scoring_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced discovery scoring failure');
             END;",
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig {
                    channel_capacity: 16,
                    batch_max_size: 8,
                    aggregate_writes_enabled: true,
                    aggregate_write_config: aggregate_write_config(),
                },
            )?;

            let failed_swap = SwapEvent {
                wallet: "wallet-gap".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-gap".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-gap-failed".to_string(),
                slot: 100,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };
            writer.write(&failed_swap).await?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        drop(trigger_conn);
        let reopen = Connection::open(Path::new(&db_path))
            .context("failed to reopen sqlite db for trigger cleanup")?;
        reopen.execute_batch("DROP TRIGGER fail_wallet_scoring_days_insert;")?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig {
                    channel_capacity: 16,
                    batch_max_size: 8,
                    aggregate_writes_enabled: true,
                    aggregate_write_config: aggregate_write_config(),
                },
            )?;

            let successful_swap = SwapEvent {
                wallet: "wallet-gap".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-gap".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-gap-success".to_string(),
                slot: 101,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };
            writer.write(&successful_swap).await?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "startup replay must clear a latched continuity gap once it reprocesses the exact failed row"
        );
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through()?,
            Some(
                DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc)
            )
        );
        let days = verify_store.load_wallet_scoring_days_since(
            DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(days.len(), 1);
        assert_eq!(
            days[0].trades, 3,
            "startup replay must materialize the previously failed row before live writes resume"
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_replays_tail_gap_before_accepting_live_writes() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-startup-replay-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let covered_swap = SwapEvent {
            wallet: "wallet-startup-replay".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-startup-replay".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-startup-covered".to_string(),
            slot: 100,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let tail_swap = SwapEvent {
            wallet: "wallet-startup-replay".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-startup-replay".to_string(),
            amount_in: 2.0,
            amount_out: 20.0,
            signature: "sig-startup-tail".to_string(),
            slot: 101,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[covered_swap.clone(), tail_swap.clone()])?;
        seed_store
            .apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        seed_store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: covered_swap.ts_utc,
            slot: covered_swap.slot,
            signature: covered_swap.signature.clone(),
        })?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig {
                channel_capacity: 16,
                batch_max_size: 8,
                aggregate_writes_enabled: true,
                aggregate_write_config: aggregate_write_config(),
            },
        )?;
        writer.shutdown()?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let days = verify_store.load_wallet_scoring_days_since(
            DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(days.len(), 1);
        assert_eq!(
            days[0].trades, 2,
            "startup replay must materialize raw tail gap"
        );
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: tail_swap.ts_utc,
                slot: tail_swap.slot,
                signature: tail_swap.signature.clone(),
            })
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_upserts_wallet_activity_days_for_inserted_swaps() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-activity-days-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig {
                    channel_capacity: 16,
                    batch_max_size: 8,
                    aggregate_writes_enabled: true,
                    aggregate_write_config: aggregate_write_config(),
                },
            )?;

            let swap_day_one = SwapEvent {
                wallet: "wallet-activity".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-activity".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-observed-swap-day-1".to_string(),
                slot: 100,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };
            let swap_day_two = SwapEvent {
                wallet: "wallet-activity".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-activity".to_string(),
                amount_in: 2.0,
                amount_out: 20.0,
                signature: "sig-observed-swap-day-2".to_string(),
                slot: 101,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };

            writer.write(&swap_day_one).await?;
            writer.write(&swap_day_two).await?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let counts = verify_store.wallet_active_day_counts_since(
            &["wallet-activity".to_string()],
            DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(counts.get("wallet-activity"), Some(&2));
        let covered_through = verify_store.load_discovery_scoring_covered_through()?;
        assert_eq!(
            covered_through,
            Some(
                DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc)
            )
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }
}
