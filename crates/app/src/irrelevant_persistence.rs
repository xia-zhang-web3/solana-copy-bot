use crate::*;

pub(crate) fn prune_noncritical_zero_universe_pending_irrelevant_swaps(
    pending_irrelevant_swaps: &mut VecDeque<PendingIrrelevantObservedSwap>,
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    app_consumer_loop_telemetry: &mut AppConsumerLoopTelemetry,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &HashSet<String>,
) -> usize {
    if !zero_universe_fail_closed_discovery_market_context_mode(
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
    ) {
        return 0;
    }

    let mut retained = VecDeque::with_capacity(pending_irrelevant_swaps.len());
    let mut dropped = 0usize;
    while let Some(mut pending) = pending_irrelevant_swaps.pop_front() {
        let still_discovery_critical =
            irrelevant_observed_swap_requires_discovery_critical_persistence(
                &pending.swap,
                follow_snapshot,
                open_shadow_lots,
                shadow_strategy_fail_closed,
                discovery_critical_target_buy_mints,
            );
        if still_discovery_critical {
            pending.discovery_critical = true;
            retained.push_back(pending);
            continue;
        }

        forget_recent_swap_signature(
            recent_signatures,
            recent_signature_order,
            &pending.swap.signature,
        );
        app_consumer_loop_telemetry.note_processing_started_at(pending.processing_started_at);
        dropped = dropped.saturating_add(1);
    }
    *pending_irrelevant_swaps = retained;
    dropped
}

pub(crate) fn enqueue_irrelevant_observed_swap_immediately(
    observed_swap_writer: &ObservedSwapWriter,
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    swap: &SwapEvent,
    discovery_critical: bool,
) -> Result<IrrelevantObservedSwapEnqueueOutcome> {
    let enqueue_result = if discovery_critical {
        observed_swap_writer.try_enqueue_discovery_critical(swap)
    } else {
        observed_swap_writer.try_enqueue(swap)
    };
    match enqueue_result {
        Ok(true) => Ok(IrrelevantObservedSwapEnqueueOutcome::Enqueued),
        Ok(false) => Ok(IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure),
        Err(error) => {
            forget_recent_swap_signature(
                recent_signatures,
                recent_signature_order,
                &swap.signature,
            );
            Err(error)
        }
    }
}

pub(crate) async fn persist_relevant_observed_swap(
    observed_swap_writer: &ObservedSwapWriter,
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    swap: &SwapEvent,
) -> Result<bool> {
    match observed_swap_writer.write(swap).await {
        Ok(inserted) => Ok(inserted),
        Err(error) => {
            forget_recent_swap_signature(
                recent_signatures,
                recent_signature_order,
                &swap.signature,
            );
            Err(error)
        }
    }
}

pub(crate) async fn persist_irrelevant_observed_swap(
    observed_swap_writer: &ObservedSwapWriter,
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    swap: &SwapEvent,
    discovery_critical: bool,
) -> Result<IrrelevantObservedSwapEnqueueOutcome> {
    enqueue_irrelevant_observed_swap(
        observed_swap_writer,
        recent_signatures,
        recent_signature_order,
        swap,
        discovery_critical,
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelevantObservedSwapPersistence {
    Inserted,
    Duplicate,
}

pub(crate) fn relevant_observed_swap_persistence(
    inserted: bool,
) -> RelevantObservedSwapPersistence {
    if inserted {
        RelevantObservedSwapPersistence::Inserted
    } else {
        RelevantObservedSwapPersistence::Duplicate
    }
}
