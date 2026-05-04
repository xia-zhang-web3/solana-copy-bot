use super::*;

impl ShadowScheduler {
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
        capacity: usize,
        task_input: ShadowTaskInput,
        holdback_ms: u64,
        now: DateTime<Utc>,
    ) -> std::result::Result<(), ShadowTaskInput> {
        if self.buffered_shadow_task_count() >= capacity {
            return Err(task_input);
        }
        self.push_held_shadow_sell(task_input, holdback_ms, now, "queued");
        Ok(())
    }

    pub(crate) fn handle_held_sell_overflow(
        &mut self,
        overflow_task: ShadowTaskInput,
        capacity: usize,
        holdback_ms: u64,
        now: DateTime<Utc>,
        shadow_drop_reason_counts: &mut BTreeMap<&'static str, u64>,
        shadow_drop_stage_counts: &mut BTreeMap<&'static str, u64>,
        shadow_queue_full_outcome_counts: &mut BTreeMap<&'static str, u64>,
    ) {
        if let Some(evicted_buy_task) = self.evict_one_pending_buy_task() {
            let sell_swap_for_log = overflow_task.swap.clone();
            if self.buffered_shadow_task_count() < capacity {
                self.push_held_shadow_sell(
                    overflow_task,
                    holdback_ms,
                    now,
                    "queued_after_buy_evict",
                );
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
                return;
            }

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
        }

        *self
            .shadow_holdback_counts
            .entry("queue_full_dropped")
            .or_insert(0) += 1;
        record_shadow_queue_full_sell_outcome(
            &overflow_task.swap,
            false,
            shadow_drop_reason_counts,
            shadow_drop_stage_counts,
            shadow_queue_full_outcome_counts,
        );
    }

    fn push_held_shadow_sell(
        &mut self,
        task_input: ShadowTaskInput,
        holdback_ms: u64,
        now: DateTime<Utc>,
        holdback_count_key: &'static str,
    ) {
        let hold_until = now + chrono::Duration::milliseconds(holdback_ms.max(1) as i64);
        self.held_shadow_sells
            .entry(task_input.key.clone())
            .or_default()
            .push_back(HeldShadowSell {
                task_input,
                hold_until,
            });
        self.held_shadow_sell_count = self.held_shadow_sell_count.saturating_add(1);
        *self
            .shadow_holdback_counts
            .entry(holdback_count_key)
            .or_insert(0) += 1;
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
                self.held_shadow_sell_count = self.held_shadow_sell_count.saturating_sub(1);
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
}
