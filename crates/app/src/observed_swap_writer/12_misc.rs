fn latch_discovery_scoring_materialization_gap_from_swaps(
    store: &SqliteStore,
    inserted_swaps: &[SwapEvent],
) -> Result<()> {
    let Some(first_gap_swap) = inserted_swaps.iter().min_by(|a, b| {
        a.ts_utc
            .cmp(&b.ts_utc)
            .then_with(|| a.slot.cmp(&b.slot))
            .then_with(|| a.signature.cmp(&b.signature))
    }) else {
        return Ok(());
    };
    store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
        ts_utc: first_gap_swap.ts_utc,
        slot: first_gap_swap.slot,
        signature: first_gap_swap.signature.clone(),
    })?;
    Ok(())
}

fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }
    "unknown panic payload".to_string()
}
