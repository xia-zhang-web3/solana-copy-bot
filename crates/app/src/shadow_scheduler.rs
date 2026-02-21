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
            held_shadow_sells: HashMap::new(),
            shadow_holdback_counts: BTreeMap::new(),
        }
    }

    pub(crate) fn key_has_pending_or_inflight(&self, key: &ShadowTaskKey) -> bool {
        self.inflight_shadow_keys.contains(key)
            || self
                .pending_shadow_tasks
                .get(key)
                .is_some_and(|pending| !pending.is_empty())
    }

    pub(crate) fn should_hold_sell_for_causality(
        &self,
        holdback_enabled: bool,
        holdback_ms: u64,
        side: ShadowSwapSide,
        key: &ShadowTaskKey,
        open_shadow_lots: &HashSet<(String, String)>,
    ) -> bool {
        if !holdback_enabled || holdback_ms == 0 || !matches!(side, ShadowSwapSide::Sell) {
            return false;
        }
        if self.key_has_pending_or_inflight(key) {
            return false;
        }
        let key_tuple = (key.wallet.clone(), key.token.clone());
        !open_shadow_lots.contains(&key_tuple)
    }

    pub(crate) fn hold_sell_for_causality(
        &mut self,
        task_input: ShadowTaskInput,
        holdback_ms: u64,
        now: DateTime<Utc>,
    ) {
        let hold_until = now + chrono::Duration::milliseconds(holdback_ms.max(1) as i64);
        self.held_shadow_sells
            .entry(task_input.key.clone())
            .or_default()
            .push_back(HeldShadowSell {
                task_input,
                hold_until,
            });
        *self.shadow_holdback_counts.entry("queued").or_insert(0) += 1;
    }

    pub(crate) fn release_held_shadow_sells(
        &mut self,
        open_shadow_lots: &HashSet<(String, String)>,
        shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
        shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
        shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
        capacity: usize,
        now: DateTime<Utc>,
    ) {
        let keys: Vec<ShadowTaskKey> = self.held_shadow_sells.keys().cloned().collect();
        for key in keys {
            loop {
                let release_reason = match self
                    .held_shadow_sells
                    .get(&key)
                    .and_then(|queue| queue.front())
                {
                    Some(front) => {
                        let key_tuple = (key.wallet.clone(), key.token.clone());
                        if open_shadow_lots.contains(&key_tuple) {
                            Some("released_open_lot")
                        } else if self.key_has_pending_or_inflight(&key) {
                            Some("released_key_busy")
                        } else if now >= front.hold_until {
                            Some("released_expired")
                        } else {
                            Some("hold")
                        }
                    }
                    None => None,
                };

                let Some(release_reason) = release_reason else {
                    break;
                };
                if release_reason == "hold" {
                    break;
                }

                let held_task = {
                    let Some(queue) = self.held_shadow_sells.get_mut(&key) else {
                        break;
                    };
                    queue.pop_front()
                };
                let Some(held_task) = held_task else {
                    break;
                };
                *self
                    .shadow_holdback_counts
                    .entry(release_reason)
                    .or_insert(0) += 1;

                if let Err(dropped_task) = self.enqueue_shadow_task(capacity, held_task.task_input)
                {
                    *self
                        .shadow_holdback_counts
                        .entry("release_enqueue_overflow")
                        .or_insert(0) += 1;
                    self.handle_shadow_enqueue_overflow(
                        ShadowSwapSide::Sell,
                        dropped_task,
                        capacity,
                        shadow_drop_reason_counts,
                        shadow_drop_stage_counts,
                        shadow_queue_full_outcome_counts,
                    );
                }
            }

            let remove_key = self
                .held_shadow_sells
                .get(&key)
                .is_some_and(|queue| queue.is_empty());
            if remove_key {
                self.held_shadow_sells.remove(&key);
            }
        }
    }

    pub(crate) fn should_process_shadow_inline(
        &self,
        shadow_queue_full: bool,
        shadow_scheduler_needs_reset: bool,
        shadow_worker_count: usize,
        key: &ShadowTaskKey,
    ) -> bool {
        shadow_queue_full
            && !shadow_scheduler_needs_reset
            && shadow_worker_count < SHADOW_MAX_CONCURRENT_WORKERS
            && !self.key_has_pending_or_inflight(key)
    }

    pub(crate) fn spawn_shadow_tasks_up_to_limit(
        &mut self,
        sqlite_path: &str,
        shadow: &copybot_shadow::ShadowService,
        max_workers: usize,
    ) {
        while self.shadow_workers.len() < max_workers {
            let Some(next) = self.dequeue_next_shadow_task() else {
                return;
            };
            spawn_shadow_worker_task(&mut self.shadow_workers, shadow, sqlite_path, next);
        }
    }

    pub(crate) fn enqueue_shadow_task(
        &mut self,
        capacity: usize,
        task_input: ShadowTaskInput,
    ) -> std::result::Result<(), ShadowTaskInput> {
        if self.pending_shadow_task_count >= capacity {
            return Err(task_input);
        }
        let key = task_input.key.clone();
        self.pending_shadow_tasks
            .entry(key.clone())
            .or_default()
            .push_back(task_input);
        self.pending_shadow_task_count = self.pending_shadow_task_count.saturating_add(1);
        if !self.inflight_shadow_keys.contains(&key)
            && self.ready_shadow_key_set.insert(key.clone())
        {
            self.ready_shadow_keys.push_back(key);
        }
        Ok(())
    }

    pub(crate) fn handle_shadow_enqueue_overflow(
        &mut self,
        overflow_side: ShadowSwapSide,
        overflow_task: ShadowTaskInput,
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
                if let Some(evicted_buy_task) = self.evict_one_pending_buy_task() {
                    let sell_swap_for_log = overflow_task.swap.clone();
                    match self.enqueue_shadow_task(capacity, overflow_task) {
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
                            if let Err(still_evicted_buy_task) =
                                self.enqueue_shadow_task(capacity, evicted_buy_task)
                            {
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

    fn evict_one_pending_buy_task(&mut self) -> Option<ShadowTaskInput> {
        let ready_candidate = self.ready_shadow_keys.iter().find_map(|key| {
            self.pending_shadow_tasks
                .get(key)
                .and_then(find_last_pending_buy_index)
                .map(|index| (key.clone(), index))
        });
        let candidate = ready_candidate.or_else(|| {
            let keys: Vec<ShadowTaskKey> = self.pending_shadow_tasks.keys().cloned().collect();
            keys.into_iter().find_map(|key| {
                self.pending_shadow_tasks
                    .get(&key)
                    .and_then(find_last_pending_buy_index)
                    .map(|index| (key, index))
            })
        })?;

        let (key, index) = candidate;
        let mut remove_key = false;
        let removed = if let Some(queue) = self.pending_shadow_tasks.get_mut(&key) {
            let task = queue.remove(index);
            remove_key = queue.is_empty();
            task
        } else {
            None
        };

        if remove_key {
            self.pending_shadow_tasks.remove(&key);
            self.ready_shadow_key_set.remove(&key);
            self.ready_shadow_keys.retain(|ready_key| ready_key != &key);
        }

        if removed.is_some() {
            self.pending_shadow_task_count = self.pending_shadow_task_count.saturating_sub(1);
        }
        removed
    }

    pub(crate) fn dequeue_next_shadow_task(&mut self) -> Option<ShadowTaskInput> {
        while let Some(key) = self.ready_shadow_keys.pop_front() {
            self.ready_shadow_key_set.remove(&key);
            if self.inflight_shadow_keys.contains(&key) {
                continue;
            }

            let mut remove_key = false;
            let next_task = if let Some(queue) = self.pending_shadow_tasks.get_mut(&key) {
                let task = queue.pop_front();
                remove_key = queue.is_empty();
                task
            } else {
                None
            };

            if remove_key {
                self.pending_shadow_tasks.remove(&key);
            }

            if let Some(task) = next_task {
                self.inflight_shadow_keys.insert(key);
                self.pending_shadow_task_count = self.pending_shadow_task_count.saturating_sub(1);
                return Some(task);
            }
        }
        None
    }

    pub(crate) fn rebuild_ready_queue(&mut self) {
        self.ready_shadow_keys.clear();
        self.ready_shadow_key_set.clear();
        for (key, pending) in &self.pending_shadow_tasks {
            if pending.is_empty() || self.inflight_shadow_keys.contains(key) {
                continue;
            }
            if self.ready_shadow_key_set.insert(key.clone()) {
                self.ready_shadow_keys.push_back(key.clone());
            }
        }
    }

    pub(crate) fn mark_task_complete(&mut self, key: &ShadowTaskKey) {
        self.inflight_shadow_keys.remove(key);
        if self
            .pending_shadow_tasks
            .get(key)
            .is_some_and(|pending| !pending.is_empty())
            && self.ready_shadow_key_set.insert(key.clone())
        {
            self.ready_shadow_keys.push_back(key.clone());
        }
    }
}
