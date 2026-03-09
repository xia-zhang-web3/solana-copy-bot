use anyhow::{anyhow, Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage::{DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, SqliteStore};
use std::path::Path;
use std::thread;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

const OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY: usize = 4096;
const OBSERVED_SWAP_BATCH_MAX_SIZE: usize = 128;
const OBSERVED_SWAP_RETENTION_SWEEP_INTERVAL: StdDuration = StdDuration::from_secs(15 * 60);
pub(crate) const OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT: &str =
    "observed swap writer channel closed";
pub(crate) const OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT: &str =
    "observed swap writer reply channel closed";

#[derive(Clone)]
struct ObservedSwapWriterConfig {
    channel_capacity: usize,
    batch_max_size: usize,
    retention_days: u32,
    aggregate_retention_days: u32,
    retention_sweep_interval: StdDuration,
    aggregate_writes_enabled: bool,
    aggregate_write_config: DiscoveryAggregateWriteConfig,
}

impl ObservedSwapWriterConfig {
    fn production(
        retention_days: u32,
        aggregate_retention_days: u32,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
    ) -> Self {
        Self {
            channel_capacity: OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            batch_max_size: OBSERVED_SWAP_BATCH_MAX_SIZE,
            retention_days,
            aggregate_retention_days,
            retention_sweep_interval: OBSERVED_SWAP_RETENTION_SWEEP_INTERVAL,
            aggregate_writes_enabled,
            aggregate_write_config,
        }
    }
}

struct ObservedSwapWriteRequest {
    swap: SwapEvent,
    reply_tx: oneshot::Sender<Result<bool>>,
}

pub(crate) struct ObservedSwapWriter {
    sender: mpsc::Sender<ObservedSwapWriteRequest>,
    worker: Option<thread::JoinHandle<Result<()>>>,
}

impl ObservedSwapWriter {
    pub(crate) fn start(
        sqlite_path: String,
        retention_days: u32,
        aggregate_retention_days: u32,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
    ) -> Result<Self> {
        Self::start_with_config(
            sqlite_path,
            ObservedSwapWriterConfig::production(
                retention_days,
                aggregate_retention_days,
                aggregate_writes_enabled,
                aggregate_write_config,
            ),
        )
    }

    fn start_with_config(sqlite_path: String, config: ObservedSwapWriterConfig) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(config.channel_capacity);
        let worker = thread::Builder::new()
            .name("copybot-observed-swap-writer".to_string())
            .spawn(move || observed_swap_writer_loop(sqlite_path, receiver, config))
            .context("failed to spawn observed swap writer thread")?;
        Ok(Self {
            sender,
            worker: Some(worker),
        })
    }

    pub(crate) async fn write(&self, swap: &SwapEvent) -> Result<bool> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(ObservedSwapWriteRequest {
                swap: swap.clone(),
                reply_tx,
            })
            .await
            .context(OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT)?;
        reply_rx
            .await
            .context(OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT)?
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
) -> Result<()> {
    let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
        format!("failed to open sqlite db for observed swap writer: {sqlite_path}")
    })?;
    run_aggregate_startup_replay(&store, &config)?;
    let mut last_retention_sweep = Instant::now()
        .checked_sub(config.retention_sweep_interval)
        .unwrap_or_else(Instant::now);

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
        for request in batch {
            swaps.push(request.swap);
            replies.push(request.reply_tx);
        }

        match store.insert_observed_swaps_batch_with_activity_days(&swaps) {
            Ok(results) => {
                for (reply_tx, inserted) in replies.into_iter().zip(results.iter().copied()) {
                    let _ = reply_tx.send(Ok(inserted));
                }
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
                for reply_tx in replies {
                    let _ = reply_tx.send(Err(anyhow!(message.clone())));
                }
            }
        }

        if last_retention_sweep.elapsed() >= config.retention_sweep_interval {
            let now = Utc::now();
            let nominal_cutoff = now - ChronoDuration::days(config.retention_days.max(1) as i64);
            let effective_cutoff = match store.load_discovery_scoring_backfill_protected_since(now)
            {
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
                        let aggregate_cutoff = now
                            - ChronoDuration::days(config.aggregate_retention_days.max(1) as i64);
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
                    if deleted_raw > 0 || deleted_scoring > 0 {
                        match store.checkpoint_wal_truncate() {
                            Ok((busy, log_frames, checkpointed_frames)) => {
                                info!(
                                    retention_days = config.retention_days,
                                    aggregate_retention_days = config.aggregate_retention_days,
                                    nominal_observed_swap_cutoff = %nominal_cutoff,
                                    effective_observed_swap_cutoff = %effective_cutoff,
                                    deleted_observed_swap_rows = deleted_raw,
                                    deleted_scoring_rows = deleted_scoring,
                                    wal_checkpoint_busy = busy,
                                    wal_log_frames = log_frames,
                                    wal_checkpointed_frames = checkpointed_frames,
                                    "observed swap retention sweep reclaimed sqlite wal"
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
                                    "observed swap retention sweep deleted rows but wal checkpoint failed"
                                );
                            }
                        }
                    }
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
            last_retention_sweep = Instant::now();
        }
    }

    Ok(())
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
                let writer = ObservedSwapWriter::start(
                    sqlite_path.clone(),
                    45,
                    45,
                    true,
                    aggregate_write_config(),
                )?;
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
    fn observed_swap_writer_applies_retention_sweep() -> Result<()> {
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
                    retention_days: 1,
                    aggregate_retention_days: 7,
                    retention_sweep_interval: StdDuration::ZERO,
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
        let swaps = verify_store.load_observed_swaps_since(Utc::now() - ChronoDuration::days(7))?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-new");
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_respects_backfill_source_protection_during_retention_sweep(
    ) -> Result<()> {
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
                    retention_days: 1,
                    aggregate_retention_days: 7,
                    retention_sweep_interval: StdDuration::ZERO,
                    aggregate_writes_enabled: false,
                    aggregate_write_config: aggregate_write_config(),
                },
            )?;

            let fresh_swap = SwapEvent {
                wallet: "wallet-protected-new".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-protected-new".to_string(),
                amount_in: 2.0,
                amount_out: 20.0,
                signature: "sig-protected-new".to_string(),
                slot: 101,
                ts_utc: Utc::now(),
                exact_amounts: None,
            };

            writer.write(&fresh_swap).await?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

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
                    retention_days: 45,
                    aggregate_retention_days: 31,
                    retention_sweep_interval: StdDuration::from_secs(3600),
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
                    retention_days: 45,
                    aggregate_retention_days: 31,
                    retention_sweep_interval: StdDuration::from_secs(3600),
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
                retention_days: 45,
                aggregate_retention_days: 31,
                retention_sweep_interval: StdDuration::from_secs(3600),
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
                    retention_days: 45,
                    aggregate_retention_days: 31,
                    retention_sweep_interval: StdDuration::from_secs(3600),
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
