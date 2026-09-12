use super::source_sell_buy_fixture::proven_buy;
use super::*;
use crate::source_sell_staging::StageCompletion;
use copybot_storage_core::ExecutionSourceSellIntent;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
static FIXTURE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub(super) struct Ingress {
    pub store: SqliteStore,
    pub path: PathBuf,
    pub now: DateTime<Utc>,
    pub scheduler: ShadowScheduler,
    pub follow: Arc<FollowSnapshot>,
    pub lots: HashSet<(String, String)>,
    pub shadow: ShadowService,
    pub writer: Option<ObservedSwapWriter>,
    pub reasons: BTreeMap<&'static str, u64>,
    pub recent: HashSet<String>,
    recent_order: VecDeque<String>,
    risk: ShadowRiskGuard,
    finished: bool,
}

impl Ingress {
    pub fn new() -> Result<Self> {
        let id = FIXTURE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (store, path) = make_test_store(&format!("source-sell-ingress-{id}"))?;
        let writer = ObservedSwapWriter::start_for_test(path.to_string_lossy().into(), 8, 8)?;
        let mut risk_config = RiskConfig::default();
        risk_config.shadow_killswitch_enabled = false;
        Ok(Self {
            store,
            path,
            writer: Some(writer),
            now: Utc::now() - chrono::Duration::seconds(10),
            scheduler: ShadowScheduler::new(),
            follow: Arc::new(FollowSnapshot::default()),
            lots: HashSet::new(),
            shadow: ShadowService::new(permissive_shadow_quality()),
            reasons: BTreeMap::new(),
            recent: HashSet::new(),
            recent_order: VecDeque::new(),
            risk: ShadowRiskGuard::new(risk_config),
            finished: false,
        })
    }
    pub fn conn(&self) -> Result<Connection> {
        Ok(Connection::open(&self.path)?)
    }
    pub fn buy(&self, id: &str, source: &str) -> Result<String> {
        proven_buy(&self.store, id, source, self.now)
    }
    pub fn position(&self) -> Result<String> {
        Ok(self
            .store
            .load_execution_canary_open_position("mint")?
            .unwrap()
            .position_id)
    }
    pub fn sell(&self, signature: &str, source: &str) -> SwapEvent {
        SwapEvent {
            signature: signature.into(),
            wallet: source.into(),
            dex: "pumpswap".into(),
            token_in: "mint".into(),
            token_out: "So11111111111111111111111111111111111111112".into(),
            amount_in: 4.0,
            amount_out: 1.0,
            slot: 42,
            ts_utc: self.now + chrono::Duration::seconds(1),
            exact_amounts: Some(copybot_core_types::ExactSwapAmounts {
                amount_in_raw: "4000".into(),
                amount_in_decimals: 3,
                amount_out_raw: "1000000000".into(),
                amount_out_decimals: 9,
            }),
        }
    }
    pub fn follow_source(&mut self, source: &str) -> Result<()> {
        self.store
            .activate_follow_wallet(source, self.now, "fixture")?;
        Arc::make_mut(&mut self.follow).active.insert(source.into());
        Ok(())
    }
    pub async fn send(&mut self, event: &SwapEvent, fail_closed: bool) -> Result<()> {
        tokio::time::timeout(
            StdDuration::from_secs(5),
            crate::app_loop_ingestion::handle_ingestion_swap_poll(
                &self.store,
                self.writer.as_ref().unwrap(),
                &ExecutionCanaryRunner::new(ExecutionConfig::default()),
                &self.shadow,
                &self.path.to_string_lossy(),
                Ok(Some(event.clone())),
                None,
                &self.follow,
                &mut self.scheduler,
                &self.lots,
                fail_closed,
                &mut self.risk,
                &OperatorEmergencyStop::from_env(),
                false,
                false,
                false,
                0,
                &mut HashSet::new(),
                &mut DiscoveryCriticalTargetBuyMintsBackpressureRefreshState::default(),
                &mut ZeroUniverseEmptyTargetNoncriticalBestEffortState::default(),
                &mut VecDeque::new(),
                &mut self.recent,
                &mut self.recent_order,
                &mut AppConsumerLoopTelemetry::default(),
                &mut self.reasons,
                &mut BTreeMap::new(),
                &mut BTreeMap::new(),
                &mut 0,
                &mut None,
            ),
        )
        .await
        .context("ingress deadline")?
    }
    pub async fn stage_completion(&mut self) -> Result<StageCompletion> {
        tokio::time::timeout(
            StdDuration::from_secs(5),
            self.scheduler.source_sells.finish_next(),
        )
        .await
        .context("staging completion deadline")?
        .context("staging completion result")?
        .context("expected worker")
    }
    pub fn staged(&self, signature: &str) -> Result<Option<ExecutionSourceSellIntent>> {
        self.store
            .load_execution_source_sell_intent(&format!("source-sell:{signature}"))
    }
    pub async fn shadow_completion(
        &mut self,
    ) -> Result<Option<copybot_shadow::ShadowSignalResult>> {
        self.scheduler.spawn_shadow_tasks_up_to_limit(
            &self.path.to_string_lossy(),
            &self.shadow,
            2,
        );
        if self.scheduler.shadow_workers.is_empty() {
            return Ok(None);
        }
        let output = tokio::time::timeout(
            StdDuration::from_secs(5),
            self.scheduler.shadow_workers.join_next(),
        )
        .await
        .context("shadow completion deadline")?;
        crate::app_loop_shadow::handle_shadow_worker_join(
            &self.store,
            output,
            &mut self.scheduler,
            &mut self.lots,
            &mut self.reasons,
            &mut BTreeMap::new(),
        )
    }
    pub fn reopen(&mut self) -> Result<()> {
        self.store = SqliteStore::open(&self.path)?;
        self.follow = Arc::new(FollowSnapshot::default());
        Ok(())
    }
    pub fn money(&self) -> Result<BTreeMap<String, Vec<String>>> {
        let conn = self.conn()?;
        let mut result = BTreeMap::new();
        for table in [
            "copy_signals",
            "orders",
            "fills",
            "positions",
            "execution_canary_receipt_proofs",
            "execution_canary_receipt_facts",
            "shadow_lots",
            "shadow_closed_trades",
            "execution_quote_canary_events",
        ] {
            let mut stmt = conn.prepare(&format!("SELECT * FROM {table}"))?;
            let n = stmt.column_count();
            let mut rows = stmt
                .query_map([], |r| {
                    let v = (0..n)
                        .map(|i| r.get::<_, rusqlite::types::Value>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    Ok(format!("{v:?}"))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows.sort();
            result.insert(table.into(), rows);
        }
        Ok(result)
    }
    pub fn pause_worker(&mut self) -> std::sync::mpsc::Sender<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.scheduler.source_sells.before_proof = Some(Box::new(move || {
            rx.recv_timeout(StdDuration::from_secs(5))
                .expect("worker fixture release deadline");
        }));
        tx
    }
    pub async fn finish(&mut self) -> Result<()> {
        tokio::time::timeout(StdDuration::from_secs(5), self.finish_inner())
            .await
            .context("whole ingress fixture shutdown deadline")?
    }
    async fn finish_inner(&mut self) -> Result<()> {
        tokio::time::timeout(
            StdDuration::from_secs(5),
            self.scheduler.source_sells.drain(),
        )
        .await
        .context("drain staging deadline")??;
        while !self.scheduler.shadow_workers.is_empty()
            || self.scheduler.pending_shadow_task_count > 0
        {
            self.shadow_completion().await?;
            tokio::task::yield_now().await;
        }
        if let Some(writer) = self.writer.take() {
            let result = tokio::time::timeout(
                StdDuration::from_secs(5),
                tokio::task::spawn_blocking(move || writer.shutdown()),
            )
            .await
            .context("writer shutdown deadline")??;
            self.finished = true;
            result?;
        }
        self.finished = true;
        Ok(())
    }
}
impl Drop for Ingress {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            assert!(self.finished, "ingress fixture requires awaited finish");
        }
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}

impl Ingress {
    pub fn root_evict_recent_with_production_dedupe(&mut self) {
        for i in 0..crate::RECENT_SWAP_SIGNATURE_DEDUPE_CAPACITY {
            assert!(crate::note_recent_swap_signature(
                &mut self.recent,
                &mut self.recent_order,
                &format!("unrelated-recent-signature-{i}"),
            ));
        }
    }
}

#[path = "source_sell_handoff_reset_fixture.rs"]
mod source_sell_handoff_reset_fixture;
