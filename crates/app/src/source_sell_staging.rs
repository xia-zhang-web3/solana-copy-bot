//! Bounded delivery hints, never cached source authority or a runnable queue.
use anyhow::Result;
use copybot_core_types::SwapEvent;
use copybot_storage_core::SqliteStore;
use std::collections::{HashMap, VecDeque};
use tokio::task::{Id, JoinSet};

use crate::shadow_scheduler::ShadowSwapSide;
use crate::source_sell_ingress::RecentSwapDelivery;
use crate::swap_classification::classify_swap_side;

#[path = "source_sell_staging_telemetry.rs"]
mod telemetry;
#[path = "source_sell_staging_worker.rs"]
mod worker;
pub(crate) use telemetry::{record, StageNotice};
pub(crate) use worker::StageCompletion;

pub(crate) const SOURCE_SELL_BINDING_CAPACITY: usize = 128;
const ACTIVE_LIMIT: usize = 1;
pub(crate) const SOURCE_SELL_RECOVERY_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(1);
#[path = "source_sell_staging_recovery.rs"]
mod recovery;

#[derive(Clone, Copy, PartialEq, Eq)]
enum OriginalObservation {
    AwaitingAck,
    Inserted,
    DuplicateUnknown,
}

impl OriginalObservation {
    fn acknowledge(self, inserted: bool) -> Self {
        match self {
            Self::AwaitingAck if inserted => Self::Inserted,
            Self::AwaitingAck => Self::DuplicateUnknown,
            // Neither later INSERT nor recent eviction can erase known Unknown;
            // Duplicate ACK must also preserve an already proven original INSERT.
            retained => retained,
        }
    }
}

#[derive(Clone)]
pub(crate) struct CapturedSourceSell {
    swap: SwapEvent,
    position_id: String,
    // Original capture survives in the committed handoff. A raw reinsert ACK
    // alone never establishes a missing original generation.
    observation: OriginalObservation,
}

pub(crate) enum SourceSellAdmission {
    NotOwnedSell,
    Captured(CapturedSourceSell),
    Refused(StageNotice),
}

pub(crate) struct SourceSellStaging {
    workers: JoinSet<worker::WorkOutput>,
    next_recovery: std::time::Instant,
    active: HashMap<Id, CapturedSourceSell>,
    bindings: HashMap<String, CapturedSourceSell>,
    order: VecDeque<String>,
    #[cfg(test)]
    pub(crate) before_proof: Option<Box<dyn FnOnce() + Send>>,
}

impl SourceSellStaging {
    pub(crate) fn new() -> Self {
        Self {
            workers: JoinSet::new(),
            next_recovery: std::time::Instant::now(),
            active: HashMap::new(),
            bindings: HashMap::new(),
            order: VecDeque::new(),
            #[cfg(test)]
            before_proof: None,
        }
    }

    pub(crate) fn capture(
        &mut self,
        store: &SqliteStore,
        delivery: &RecentSwapDelivery<'_>,
    ) -> Result<SourceSellAdmission> {
        let swap = delivery.swap();
        // Durable original generation wins after restart/recent eviction. This is
        // still only a delivery candidate, never cached source authority.
        if let Some(job) = store.load_source_sell_handoff(&swap.signature)? {
            if !same_event(&job.event, swap) {
                return Ok(SourceSellAdmission::Refused(StageNotice::IdentityConflict));
            }
            return Ok(match job.original_position_id {
                None => SourceSellAdmission::Refused(StageNotice::GenerationUnknown),
                Some(position_id) => SourceSellAdmission::Captured(CapturedSourceSell {
                    swap: job.event,
                    position_id,
                    observation: OriginalObservation::Inserted,
                }),
            });
        }
        // Check retained identity first: a changed payload must not replace A.
        if let Some(captured) = self.bindings.get(&swap.signature).or_else(|| {
            self.active
                .values()
                .find(|c| c.swap.signature == swap.signature)
        }) {
            return Ok(if !same_event(&captured.swap, swap) {
                SourceSellAdmission::Refused(StageNotice::IdentityConflict)
            } else if captured.observation == OriginalObservation::Inserted
                || (captured.observation == OriginalObservation::AwaitingAck && delivery.is_fresh())
            {
                SourceSellAdmission::Captured(captured.clone())
            } else {
                SourceSellAdmission::Refused(StageNotice::GenerationUnknown)
            });
        }
        if classify_swap_side(swap) != Some(ShadowSwapSide::Sell) {
            return Ok(SourceSellAdmission::NotOwnedSell);
        }
        // Recent dedupe can outlive both the hint and observed retention. A
        // known repeat must not capture current B, even if its next ACK inserts.
        if !delivery.is_fresh() {
            return Ok(SourceSellAdmission::Refused(StageNotice::GenerationUnknown));
        }
        // A small current-position lookup only; no attribution or receipt scan.
        let Some(position) = store.load_execution_canary_open_position(&swap.token_in)? else {
            return Ok(SourceSellAdmission::NotOwnedSell);
        };
        if !position.qty.is_finite()
            || position.qty <= 1e-12
            || position.qty_exact.is_some_and(|q| q.raw() == 0)
        {
            return Ok(SourceSellAdmission::NotOwnedSell);
        }
        let captured = CapturedSourceSell {
            swap: swap.clone(),
            position_id: position.position_id,
            observation: OriginalObservation::AwaitingAck,
        };
        self.bindings
            .insert(swap.signature.clone(), captured.clone());
        self.order.push_back(swap.signature.clone());
        while self.order.len() > SOURCE_SELL_BINDING_CAPACITY {
            if let Some(evicted) = self.order.pop_front() {
                self.bindings.remove(&evicted);
            }
        }
        Ok(SourceSellAdmission::Captured(captured))
    }

    pub(crate) fn acknowledge_and_schedule(
        &mut self,
        mut captured: CapturedSourceSell,
        inserted: bool,
        store: &SqliteStore,
        sqlite_path: &str,
    ) -> Result<StageNotice> {
        captured.observation = captured.observation.acknowledge(inserted);
        if let Some(saved) = self.bindings.get_mut(&captured.swap.signature) {
            saved.observation = saved.observation.acknowledge(inserted);
        }
        if captured.observation != OriginalObservation::Inserted {
            return Ok(StageNotice::GenerationUnknown);
        }
        // ACK may have awaited across worker completion. Give the same durable
        // cursor its bounded visit before fresh ingress can claim this slot.
        if let Some(signature) = self.recover_next(store, sqlite_path)? {
            if signature == captured.swap.signature {
                return Ok(StageNotice::Scheduled);
            }
            record(&signature, StageNotice::Scheduled, None);
        }
        if self
            .active
            .values()
            .any(|a| a.swap.signature == captured.swap.signature)
        {
            return Ok(StageNotice::InFlight);
        }
        if self.workers.len() >= ACTIVE_LIMIT {
            return Ok(StageNotice::WorkerCapacity);
        }
        self.spawn(captured, sqlite_path);
        Ok(StageNotice::Scheduled)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    pub(crate) fn reap_ready(&mut self) -> Result<()> {
        while let Some(output) = self.workers.try_join_next_with_id() {
            self.complete(output)?;
        }
        Ok(())
    }

    pub(crate) async fn finish_next(&mut self) -> Result<Option<StageCompletion>> {
        match self.workers.join_next_with_id().await {
            Some(output) => self.complete(output).map(Some),
            None => Ok(None),
        }
    }

    pub(crate) async fn drain(&mut self) -> Result<()> {
        while !self.is_empty() {
            self.finish_next().await?;
        }
        Ok(())
    }
}

fn same_event(a: &SwapEvent, b: &SwapEvent) -> bool {
    a.signature == b.signature
        && a.wallet == b.wallet
        && a.dex == b.dex
        && a.token_in == b.token_in
        && a.token_out == b.token_out
        && a.slot == b.slot
        && a.ts_utc == b.ts_utc
        && a.amount_in == b.amount_in
        && a.amount_out == b.amount_out
        && a.exact_amounts == b.exact_amounts
}

impl CapturedSourceSell {
    pub(crate) fn candidate(&self) -> copybot_storage_core::SourceSellCandidate {
        copybot_storage_core::SourceSellCandidate::new(&self.swap, &self.position_id)
    }
}
