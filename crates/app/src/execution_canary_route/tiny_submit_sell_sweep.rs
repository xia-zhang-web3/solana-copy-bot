use super::tiny_submit_sell::process_tiny_submit_sell_quote_event;
use super::tiny_submit_sell_retry::{
    hold_terminal_failed_sell_simulation, terminal_failed_sell_simulation,
};
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_canary_summary::SourceSellWriteOffRefusals;
use crate::execution_source_sell_continuation::{
    Continuation, Family, SOURCE_REFUSAL_VISIT_BUDGET,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{ExecutionFailedSellSweepVisit as Visit, SqliteStore};

// At most this many indexed raw visits and local write-off checks; at most one
// non-refused handler; refused write-offs never sign/submit a transaction.
pub(super) const FAILED_SELL_SWEEP_VISIT_BUDGET: usize = 8;

pub(super) async fn process_failed_sell_simulation_sweep_for_route(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
    progress: &Continuation,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let mut collected = Summaries::default();
    // Same family priority; only source/write-off refusals permit additional work.
    for _ in 0..SOURCE_REFUSAL_VISIT_BUDGET {
        let Some((family, row)) = next_priority(config, store, now, progress)? else {
            break;
        };
        let state = process_tiny_submit_sell_quote_event(config, store, &row.event_id, now)
            .await?
            .unwrap_or_default();
        let refused = collected.push(state);
        progress.visited(family, row.cursor, refused);
        if !refused {
            return Ok(collected.finish());
        }
    }
    for _ in 0..FAILED_SELL_SWEEP_VISIT_BUDGET {
        let id = match store.advance_execution_failed_sell_sweep(&config.canary_route)? {
            Visit::Wrapped => break,
            Visit::SkippedOtherRoute => continue,
            Visit::Order(id) => id,
        };
        let Some(selected) =
            store.load_execution_failed_sell_sweep_order(&config.canary_route, &id)?
        else {
            continue;
        };
        let state = if let Some(event) = selected.quote_event_id {
            process_tiny_submit_sell_quote_event(config, store, &event, now)
                .await?
                .unwrap_or_default()
        } else if terminal_failed_sell_simulation(config, &selected.order) {
            hold_terminal_failed_sell_simulation(
                store,
                &selected.order,
                config.max_submit_attempts,
                now,
            )?
        } else {
            let Some(metadata) = store.load_execution_canary_build_plan_metadata(&id)? else {
                continue;
            };
            let Some(event) = metadata
                .quote_event_id
                .as_deref()
                .filter(|e| !e.trim().is_empty())
            else {
                continue;
            };
            process_tiny_submit_sell_quote_event(config, store, event, now)
                .await?
                .unwrap_or_default()
        };
        if !collected.push(state) {
            break;
        }
    }
    Ok(collected.finish())
}

#[derive(Default)]
struct Summaries {
    last: ExecutionCanaryStateMachineSummary,
    source: SourceSellWriteOffRefusals,
    write_off: SourceSellWriteOffRefusals,
    existing: usize,
    candidates: usize,
}
impl Summaries {
    fn push(&mut self, mut state: ExecutionCanaryStateMachineSummary) -> bool {
        let refused = state.source_sell_refusals.count() > 0
            || state.source_sell_write_off_refusals.count() > 0;
        self.source
            .merge(std::mem::take(&mut state.source_sell_refusals));
        self.write_off
            .merge(std::mem::take(&mut state.source_sell_write_off_refusals));
        self.existing += state.existing;
        self.candidates += state.sell_candidates;
        self.last = state;
        refused
    }
    fn finish(mut self) -> ExecutionCanaryStateMachineSummary {
        self.last.source_sell_refusals = self.source;
        self.last.source_sell_write_off_refusals = self.write_off;
        self.last.existing = self.existing;
        self.last.sell_candidates = self.candidates;
        self.last
    }
}
fn next_priority(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
    progress: &Continuation,
) -> Result<Option<(Family, copybot_storage_core::ExecutionRetryQuote)>> {
    use super::tiny_submit_sell_retry::{
        RETRY_FAILED_SELL_WITH_OWNED_POSITION_AMOUNT_REASON as RETRY,
        TERMINAL_SELL_NO_ROUTE_REPROBE_COOLDOWN_SECONDS as COOLDOWN,
    };
    let after = progress.after(Family::Candidate);
    if let Some(row) = store
        .list_retry_candidate_sell_execution_quote_event_ids_for_route_page(
            &config.canary_route,
            RETRY,
            1,
            after.as_ref(),
        )?
        .pop()
    {
        return Ok(Some((Family::Candidate, row)));
    }
    progress.wrap(Family::Candidate);
    let after = progress.after(Family::Cooldown);
    if let Some(row) = store
        .list_terminal_no_route_sell_execution_quote_event_ids_for_route_page(
            &config.canary_route,
            now - chrono::Duration::seconds(COOLDOWN),
            1,
            after.as_ref(),
        )?
        .pop()
    {
        return Ok(Some((Family::Cooldown, row)));
    }
    progress.wrap(Family::Cooldown);
    Ok(None)
}
