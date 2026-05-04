use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use copybot_shadow::{FollowSnapshot, ShadowSnapshot};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::task::{JoinHandle, JoinSet};

use super::SHADOW_MAX_CONCURRENT_WORKERS;
use crate::shadow_runtime_helpers::{find_last_pending_buy_index, spawn_shadow_worker_task};
use crate::telemetry::{record_shadow_queue_full_buy_drop, record_shadow_queue_full_sell_outcome};

pub(crate) struct ShadowScheduler {
    pub(crate) shadow_workers: JoinSet<ShadowTaskOutput>,
    pub(crate) shadow_snapshot_handle: Option<JoinHandle<Result<ShadowSnapshot>>>,
    pub(crate) pending_shadow_tasks: HashMap<ShadowTaskKey, VecDeque<ShadowTaskInput>>,
    pub(crate) pending_shadow_task_count: usize,
    pub(crate) ready_shadow_keys: VecDeque<ShadowTaskKey>,
    pub(crate) ready_shadow_key_set: HashSet<ShadowTaskKey>,
    pub(crate) inflight_shadow_keys: HashSet<ShadowTaskKey>,
    pub(crate) shadow_queue_backpressure_active: bool,
    pub(crate) shadow_scheduler_needs_reset: bool,
    held_shadow_sell_count: usize,
    held_shadow_sells: HashMap<ShadowTaskKey, VecDeque<HeldShadowSell>>,
    pub(crate) shadow_holdback_counts: BTreeMap<&'static str, u64>,
}

pub(crate) struct ShadowTaskOutput {
    pub(crate) signature: String,
    pub(crate) key: ShadowTaskKey,
    pub(crate) outcome: Result<copybot_shadow::ShadowProcessOutcome>,
}

pub(crate) struct ShadowTaskInput {
    pub(crate) swap: SwapEvent,
    pub(crate) follow_snapshot: Arc<FollowSnapshot>,
    pub(crate) key: ShadowTaskKey,
}

struct HeldShadowSell {
    task_input: ShadowTaskInput,
    hold_until: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ShadowTaskKey {
    pub(crate) wallet: String,
    pub(crate) token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShadowSwapSide {
    Buy,
    Sell,
}

impl ShadowScheduler {
    pub(crate) fn new() -> Self {
        Self {
            shadow_workers: JoinSet::new(),
            shadow_snapshot_handle: None,
            pending_shadow_tasks: HashMap::new(),
            pending_shadow_task_count: 0,
            ready_shadow_keys: VecDeque::new(),
            ready_shadow_key_set: HashSet::new(),
            inflight_shadow_keys: HashSet::new(),
            shadow_queue_backpressure_active: false,
            shadow_scheduler_needs_reset: false,
            held_shadow_sell_count: 0,
            held_shadow_sells: HashMap::new(),
            shadow_holdback_counts: BTreeMap::new(),
        }
    }

    pub(crate) fn buffered_shadow_task_count(&self) -> usize {
        self.pending_shadow_task_count
            .saturating_add(self.held_shadow_sell_count)
    }

    pub(crate) fn held_shadow_sell_count(&self) -> usize {
        self.held_shadow_sell_count
    }

    pub(crate) fn key_has_pending_or_inflight(&self, key: &ShadowTaskKey) -> bool {
        self.inflight_shadow_keys.contains(key)
            || self
                .pending_shadow_tasks
                .get(key)
                .is_some_and(|pending| !pending.is_empty())
    }
}

#[path = "shadow_scheduler_holdback.rs"]
mod holdback;
#[path = "shadow_scheduler_queue.rs"]
mod queue;
