use super::*;
use crate::observed_swap_writer::ObservedSwapWriteRequest;
use crate::*;
use copybot_ingestion::transaction_membership as membership;
use serde_json::Value;
use yellowstone_grpc_proto::prelude::*;

pub fn chosen(n: u8) -> SwapEvent {
    let mut row = recent_raw_journal_backpressure_swap(n as usize, Utc::now());
    row.signature = bs58::encode(vec![n; 64]).into_string();
    row.slot = (1..100_000)
        .find(|s| membership::selected(&row.signature, *s))
        .unwrap();
    row.token_in = "So11111111111111111111111111111111111111112".into();
    row.token_out = "test-target".into();
    row
}

pub fn note(row: &SwapEvent) {
    membership::decoded_update(&SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Transaction(
            SubscribeUpdateTransaction {
                slot: row.slot,
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: bs58::decode(&row.signature).into_vec().unwrap(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
        ..Default::default()
    });
}

pub fn count(snapshot: &Value, n: u8, outcome: membership::Disposition) -> u64 {
    let row = chosen(n);
    snapshot["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["signature"] == row.signature)
        .and_then(|r| r["outcomes"][outcome.code().to_string()].as_u64())
        .unwrap_or(0)
}

pub struct Case {
    pub path: std::path::PathBuf,
    pub store: SqliteStore,
    pub targets: HashSet<String>,
    pub pending: VecDeque<PendingIrrelevantObservedSwap>,
    pub budget: ZeroUniverseEmptyTargetNoncriticalBestEffortState,
    refresh: DiscoveryCriticalTargetBuyMintsBackpressureRefreshState,
    recent: HashSet<String>,
    order: VecDeque<String>,
}

impl Case {
    pub fn critical(&mut self) {
        self.targets.insert("test-target".into());
        self.refresh.note_refresh_attempt(StdInstant::now());
    }

    pub fn new(n: u8) -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "membership-disposition-{}-{n}.db",
            std::process::id()
        ));
        let store = prepare_observed_writer_store_for_test(&path)?;
        let mut refresh = DiscoveryCriticalTargetBuyMintsBackpressureRefreshState::default();
        // Inject the current cached set, without changing the real refresh predicate or loader.
        refresh.note_refresh_attempt(StdInstant::now());
        Ok(Self {
            path,
            store,
            targets: HashSet::new(),
            pending: VecDeque::new(),
            budget: Default::default(),
            refresh,
            recent: HashSet::new(),
            order: VecDeque::new(),
        })
    }

    pub async fn handle(
        &mut self,
        writer: &ObservedSwapWriter,
        row: SwapEvent,
        output_pressure: bool,
    ) -> Result<()> {
        let snapshot = output_pressure.then_some(IngestionRuntimeSnapshot {
            ts_utc: Utc::now(),
            ws_notifications_enqueued: 0,
            ws_notifications_replaced_oldest: 0,
            grpc_message_total: 0,
            grpc_transaction_updates_total: 0,
            parse_rejected_total: 0,
            grpc_decode_errors: 0,
            rpc_429: 0,
            rpc_5xx: 0,
            ingestion_lag_ms_p95: 0,
            yellowstone_output_queue_depth: 100,
            yellowstone_output_queue_capacity: 100,
            yellowstone_output_queue_fill_ratio: 1.,
            yellowstone_output_oldest_age_ms: 0,
        });
        crate::app_loop_irrelevant_swap::handle_irrelevant_observed_swap(
            &self.store,
            writer,
            row,
            IrrelevantObservedSwapBackpressureSourceBranch::NotFollowed,
            snapshot,
            &Arc::new(FollowSnapshot::default()),
            &HashSet::new(),
            true,
            &mut self.targets,
            &mut self.refresh,
            &mut self.budget,
            &mut self.pending,
            &mut self.recent,
            &mut self.order,
            &mut AppConsumerLoopTelemetry::default(),
            StdInstant::now(),
        )
        .await
    }

    pub fn paused_writer(
        &self,
    ) -> (
        ObservedSwapWriter,
        tokio::sync::mpsc::Receiver<ObservedSwapWriteRequest>,
    ) {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        (
            ObservedSwapWriter {
                sender,
                normal_try_enqueue_soft_limit: 1,
                raw_worker: None,
                journal_worker: None,
                telemetry: Arc::new(ObservedSwapWriterTelemetry::default()),
                terminal_failure_message: Arc::new(Mutex::new(None)),
            },
            receiver,
        )
    }

    pub fn start_receiver(
        &self,
        writer: &mut ObservedSwapWriter,
        receiver: tokio::sync::mpsc::Receiver<ObservedSwapWriteRequest>,
    ) {
        let path = self.path.to_string_lossy().into_owned();
        let telemetry = writer.telemetry.clone();
        let failure = writer.terminal_failure_message.clone();
        writer.raw_worker = Some(thread::spawn(move || {
            super::super::super::observed_swap_writer_loop(
                path,
                receiver,
                None,
                None,
                ObservedSwapWriterConfig::for_test(1, 1, None),
                telemetry,
                failure,
            )
        }));
    }

    pub fn rows(&self) -> Result<Vec<String>> {
        let conn = Connection::open(&self.path)?;
        let mut statement = conn.prepare("SELECT signature FROM observed_swaps ORDER BY rowid")?;
        let result = statement
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;
        Ok(result)
    }

    pub async fn retry_buffer(&mut self, writer: &ObservedSwapWriter) -> Result<()> {
        let pending = self.pending.pop_front().unwrap();
        timeout(Duration::from_secs(3), async {
            loop {
                let result = crate::enqueue_irrelevant_observed_swap(
                    writer,
                    &mut self.recent,
                    &mut self.order,
                    &pending.swap,
                    pending.discovery_critical,
                )
                .await?;
                if matches!(result, IrrelevantObservedSwapEnqueueOutcome::Enqueued) {
                    return Ok(());
                }
                sleep(Duration::from_millis(2)).await;
            }
        })
        .await?
    }
}

impl Drop for Case {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}
