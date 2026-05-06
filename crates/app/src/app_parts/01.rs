fn risk_event_write_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn shadow_risk_pause_restore_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn shadow_risk_background_refresh_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn shadow_open_lot_refresh_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn shadow_snapshot_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn shadow_risk_state_event_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn persist_runtime_risk_event_or_warn(
    store: &SqliteStore,
    event_type: &str,
    severity: &str,
    ts: DateTime<Utc>,
    details_json: Option<&str>,
    warning_message: &'static str,
    fatal_context: &'static str,
) -> Result<()> {
    if let Err(error) = store.insert_risk_event(event_type, severity, ts, details_json) {
        if risk_event_write_error_requires_restart(&error) {
            return Err(error).context(fatal_context);
        }
        warn!(
            error = %error,
            event_type,
            severity,
            warning_message,
            "failed to persist runtime risk event"
        );
    }
    Ok(())
}

fn persist_shadow_risk_fail_closed_event_or_warn(
    store: &SqliteStore,
    ts: DateTime<Utc>,
    details_json: &str,
) -> Result<()> {
    persist_runtime_risk_event_or_warn(
        store,
        "shadow_risk_fail_closed",
        "error",
        ts,
        Some(details_json),
        "failed to persist shadow risk fail-closed event",
        "failed to persist shadow risk fail-closed event with fatal sqlite I/O",
    )
}

fn observed_swap_writer_error_requires_restart(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        message == OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT
            || message == OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT
            || message == OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT
    })
}

fn ingestion_error_backoff_ms(consecutive_errors: u32) -> u64 {
    let index =
        (consecutive_errors.saturating_sub(1) as usize).min(INGESTION_ERROR_BACKOFF_MS.len() - 1);
    INGESTION_ERROR_BACKOFF_MS[index]
}

fn note_recent_swap_signature(
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    signature: &str,
) -> bool {
    if !recent_signatures.insert(signature.to_string()) {
        return false;
    }
    recent_signature_order.push_back(signature.to_string());
    while recent_signature_order.len() > RECENT_SWAP_SIGNATURE_DEDUPE_CAPACITY {
        if let Some(evicted) = recent_signature_order.pop_front() {
            recent_signatures.remove(&evicted);
        }
    }
    true
}

fn forget_recent_swap_signature(
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    signature: &str,
) {
    if !recent_signatures.remove(signature) {
        return;
    }
    if recent_signature_order
        .back()
        .is_some_and(|queued| queued == signature)
    {
        recent_signature_order.pop_back();
        return;
    }
    if let Some(position) = recent_signature_order
        .iter()
        .position(|queued| queued == signature)
    {
        recent_signature_order.remove(position);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservedSwapShadowRelevance {
    Relevant(ShadowSwapSide),
    IrrelevantUnclassified,
    IrrelevantNotFollowed(ShadowSwapSide),
}

fn classify_observed_swap_shadow_relevance(
    swap: &SwapEvent,
    follow_snapshot: &FollowSnapshot,
    shadow_scheduler: &ShadowScheduler,
    open_shadow_lots: &HashSet<(String, String)>,
) -> ObservedSwapShadowRelevance {
    let Some(side) = classify_swap_side(swap) else {
        return ObservedSwapShadowRelevance::IrrelevantUnclassified;
    };

    if matches!(side, ShadowSwapSide::Buy)
        && !follow_snapshot.is_followed_at(&swap.wallet, swap.ts_utc)
    {
        return ObservedSwapShadowRelevance::IrrelevantNotFollowed(side);
    }

    if matches!(side, ShadowSwapSide::Sell)
        && !follow_snapshot.is_followed_at(&swap.wallet, swap.ts_utc)
    {
        let wallet_has_recent_follow_history = follow_snapshot.is_active(&swap.wallet)
            || follow_snapshot.promoted_at.contains_key(&swap.wallet)
            || follow_snapshot.demoted_at.contains_key(&swap.wallet);
        let sell_key = shadow_task_key_for_swap(swap, side);
        let key_tuple = (sell_key.wallet.clone(), sell_key.token.clone());
        let has_pending_or_inflight = shadow_scheduler.key_has_pending_or_inflight(&sell_key);
        if !wallet_has_recent_follow_history
            && !has_pending_or_inflight
            && !open_shadow_lots.contains(&key_tuple)
        {
            return ObservedSwapShadowRelevance::IrrelevantNotFollowed(side);
        }
    }

    ObservedSwapShadowRelevance::Relevant(side)
}
async fn enqueue_irrelevant_observed_swap(
    observed_swap_writer: &ObservedSwapWriter,
    recent_signatures: &mut HashSet<String>,
    recent_signature_order: &mut VecDeque<String>,
    swap: &SwapEvent,
    discovery_critical: bool,
) -> Result<IrrelevantObservedSwapEnqueueOutcome> {
    enqueue_irrelevant_observed_swap_immediately(
        observed_swap_writer,
        recent_signatures,
        recent_signature_order,
        swap,
        discovery_critical,
    )
}

fn irrelevant_observed_swap_requires_discovery_critical_persistence(
    swap: &SwapEvent,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &HashSet<String>,
) -> bool {
    if !zero_universe_fail_closed_discovery_market_context_mode(
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
    ) {
        return false;
    }

    if discovery_critical_target_buy_mints.is_empty() {
        return false;
    }

    match classify_swap_side(swap) {
        Some(ShadowSwapSide::Buy) => {
            discovery_critical_target_buy_mints.contains(swap.token_out.as_str())
        }
        Some(ShadowSwapSide::Sell) => {
            discovery_critical_target_buy_mints.contains(swap.token_in.as_str())
        }
        None => false,
    }
}
