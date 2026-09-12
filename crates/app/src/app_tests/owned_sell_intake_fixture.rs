use super::open_risk_sell_fixture::{Fixture, TOKEN};
use super::*;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;

pub(super) struct Intake {
    pub f: Fixture,
    pub scheduler: ShadowScheduler,
    pub lots: HashSet<(String, String)>,
    pub reasons: BTreeMap<&'static str, u64>,
    stages: BTreeMap<&'static str, u64>,
    overflow: BTreeMap<&'static str, u64>,
    recent: HashSet<String>,
    recent_order: VecDeque<String>,
    writer: Option<ObservedSwapWriter>,
}
impl Intake {
    pub async fn finish(&mut self) -> Result<()> {
        self.f.finish().await
    }

    pub async fn new(notional: f64, lag: i64, price: u64) -> Result<Self> {
        Self::from_fixture(Fixture::new(price).await?, notional, lag)
    }
    pub async fn legacy(notional: f64, lag: i64, price: u64) -> Result<Self> {
        Self::from_fixture(Fixture::legacy(price).await?, notional, lag)
    }
    fn from_fixture(mut f: Fixture, notional: f64, lag: i64) -> Result<Self> {
        for lot in f.store.list_shadow_lots("leader", TOKEN)? {
            f.store.delete_shadow_lot(lot.id)?;
        }
        f.swap.amount_out = notional;
        f.swap.exact_amounts.as_mut().unwrap().amount_out_raw =
            ((notional * 1e9) as u64).to_string();
        f.swap.ts_utc = f.now - chrono::Duration::seconds(lag);
        let writer = ObservedSwapWriter::start_for_test(
            f.dir.join("test.db").to_string_lossy().into(),
            8,
            8,
        )?;
        Ok(Self {
            f,
            scheduler: ShadowScheduler::new(),
            lots: HashSet::new(),
            reasons: BTreeMap::new(),
            stages: BTreeMap::new(),
            overflow: BTreeMap::new(),
            recent: HashSet::new(),
            recent_order: VecDeque::new(),
            writer: Some(writer),
        })
    }
    pub fn id(&self) -> String {
        format!(
            "shadow:{}:leader:sell:{}",
            self.f.swap.signature, self.f.swap.token_in
        )
    }
    pub fn quote_id(&self) -> String {
        format!("quote:owned-close:{}", self.id())
    }
    pub fn conn(&self) -> Result<rusqlite::Connection> {
        Ok(rusqlite::Connection::open(self.f.dir.join("test.db"))?)
    }
    pub fn counts(&self) -> Result<(u64, u64, u64, u64)> {
        Ok(self.conn()?.query_row("SELECT (SELECT COUNT(*) FROM shadow_lots), (SELECT COUNT(*) FROM shadow_closed_trades), (SELECT COUNT(*) FROM copy_signals), (SELECT COUNT(*) FROM orders)", [], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?)
    }
    pub async fn dispatch(&mut self, fail_closed: bool, holdback: bool) -> Result<()> {
        let follow = Arc::new(self.f.follow.clone());
        let relevance = crate::runtime_helpers::classify_observed_swap_shadow_relevance(
            &self.f.swap,
            &follow,
            &self.scheduler,
            &self.lots,
        );
        let ObservedSwapShadowRelevance::Relevant(side) = relevance else {
            return Ok(());
        };
        let mut risk = ShadowRiskGuard::new(RiskConfig::default());
        let mut telemetry = AppConsumerLoopTelemetry::default();
        crate::app_loop_relevant_swap::handle_relevant_observed_swap(
            &self.f.store,
            self.writer.as_ref().unwrap(),
            &ExecutionCanaryRunner::new(self.f.config.clone()),
            &self.f.service,
            &self.f.dir.join("test.db").to_string_lossy(),
            self.f.swap.clone(),
            side,
            self.f.now,
            &follow,
            &self.lots,
            fail_closed,
            &mut risk,
            &OperatorEmergencyStop::from_env(),
            false,
            &mut self.scheduler,
            false,
            holdback,
            250,
            &mut self.reasons,
            &mut self.stages,
            &mut self.overflow,
            &mut self.recent,
            &mut self.recent_order,
            &mut telemetry,
            StdInstant::now(),
            None,
        )
        .await
    }
    pub async fn drain(&mut self) -> Result<Option<copybot_shadow::ShadowSignalResult>> {
        self.scheduler.spawn_shadow_tasks_up_to_limit(
            &self.f.dir.join("test.db").to_string_lossy(),
            &self.f.service,
            2,
        );
        if self.scheduler.shadow_workers.is_empty() {
            return Ok(None);
        }
        let joined = self.scheduler.shadow_workers.join_next().await;
        crate::app_loop_shadow::handle_shadow_worker_join(
            &self.f.store,
            joined,
            &mut self.scheduler,
            &mut self.lots,
            &mut self.reasons,
            &mut self.stages,
        )
    }
    pub fn release(&mut self) {
        self.scheduler.release_held_shadow_sells(
            &self.lots,
            &mut self.reasons,
            &mut self.stages,
            &mut self.overflow,
            100,
            Utc::now().max(self.f.now) + chrono::Duration::seconds(1),
        );
    }
    pub async fn hot(
        &self,
        signal: &copybot_shadow::ShadowSignalResult,
    ) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
        ExecutionCanaryRunner::new(self.f.config.clone())
            .process_recorded_shadow_signal(&self.f.store, signal, self.f.now)
            .await
    }
    pub async fn tick(&self) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
        ExecutionCanaryRunner::new(self.f.config.clone())
            .process_tick(&self.f.store, self.f.now)
            .await
    }
}
impl Drop for Intake {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            let _ = writer.shutdown();
        }
    }
}
