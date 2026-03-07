use anyhow::{anyhow, Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage::SqliteStore;
use std::path::Path;
use std::thread;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

const OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY: usize = 4096;
const OBSERVED_SWAP_BATCH_MAX_SIZE: usize = 128;
const OBSERVED_SWAP_RETENTION_SWEEP_INTERVAL: StdDuration = StdDuration::from_secs(15 * 60);

#[derive(Clone, Copy)]
struct ObservedSwapWriterConfig {
    channel_capacity: usize,
    batch_max_size: usize,
    retention_days: u32,
    retention_sweep_interval: StdDuration,
}

impl ObservedSwapWriterConfig {
    fn production(retention_days: u32) -> Self {
        Self {
            channel_capacity: OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            batch_max_size: OBSERVED_SWAP_BATCH_MAX_SIZE,
            retention_days,
            retention_sweep_interval: OBSERVED_SWAP_RETENTION_SWEEP_INTERVAL,
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
    pub(crate) fn start(sqlite_path: String, retention_days: u32) -> Result<Self> {
        Self::start_with_config(
            sqlite_path,
            ObservedSwapWriterConfig::production(retention_days),
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
            .context("observed swap writer channel closed")?;
        reply_rx
            .await
            .context("observed swap writer reply channel closed")?
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

        match store.insert_observed_swaps_batch(&swaps) {
            Ok(results) => {
                for (reply_tx, inserted) in replies.into_iter().zip(results.into_iter()) {
                    let _ = reply_tx.send(Ok(inserted));
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
            let cutoff = Utc::now() - ChronoDuration::days(config.retention_days.max(1) as i64);
            if let Err(error) = store.delete_observed_swaps_before(cutoff) {
                warn!(
                    error = %error,
                    retention_days = config.retention_days,
                    "observed swap retention sweep failed"
                );
            }
            last_retention_sweep = Instant::now();
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
    use copybot_storage::SqliteStore;
    use rusqlite::Connection;
    use std::path::Path;
    use std::thread;
    use std::time::Duration as StdDuration;
    use tokio::runtime::Builder;
    use tokio::time::{sleep, timeout, Duration};

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
                let writer = ObservedSwapWriter::start(sqlite_path.clone(), 45)?;
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
                    retention_sweep_interval: StdDuration::ZERO,
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
}
