use crate::{
    source_sell_event, source_sell_intent_rows, source_sell_promotion_rows as rows,
    source_sell_validation, ExecutionSourceSellPromotion, ExecutionSourceSellReject as Reject,
    SqliteDiscoveryStore, EXECUTION_SELL_INTENT_STATUS,
};
use anyhow::{ensure, Context, Result};
use copybot_core_types::CopySignalRow;
use rusqlite::Connection;

pub(crate) fn binding_for_signal(
    store: &SqliteDiscoveryStore,
    conn: &Connection,
    signal_id: &str,
) -> Result<Option<ExecutionSourceSellPromotion>> {
    if let Some(binding) = rows::load(conn, Some(signal_id), None)? {
        return Ok(Some(binding));
    }
    let Some(event_key) = signal_id.strip_prefix("shadow:") else {
        return Ok(None);
    };
    // Use durable fields, never caller payload/status, to derive a bounded lookup key.
    // Removing a known suffix also preserves signatures/wallets containing separators.
    let saved = store
        .load_copy_signal_by_signal_id(signal_id)?
        .context("source SELL association lookup signal missing")?;
    let suffix = format!(":{}:sell:{}", saved.wallet_id, saved.token);
    let Some(signature) = event_key.strip_suffix(&suffix).filter(|s| !s.is_empty()) else {
        ensure!(
            saved.side != "sell",
            "source SELL association lookup canonical signal conflict"
        );
        return Ok(None);
    };
    let intent_id = source_sell_event::intent_id(signature);
    let Some(binding) = rows::load(conn, None, Some(&intent_id))? else {
        // Staging alone is not a promotion marker and cannot capture legacy signals.
        return Ok(None);
    };
    let staged = source_sell_intent_rows::load(conn, &intent_id)?
        .context("source SELL reverse association staging missing")?;
    ensure!(
        source_sell_event::signal_id(&staged.event) == signal_id,
        "source SELL reverse association canonical identity conflict"
    );
    // Parsing only found the row. The existing guard validates the durable binding,
    // persisted canonical signal, generation and witness in this same snapshot.
    Ok(Some(binding))
}

pub(crate) fn block_reason(
    store: &SqliteDiscoveryStore,
    conn: &Connection,
    caller: &CopySignalRow,
    binding: &ExecutionSourceSellPromotion,
) -> Result<Option<&'static str>> {
    // Check both directions, including malformed tables that lost UNIQUE enforcement.
    if rows::load(conn, Some(&binding.signal_id), Some(&binding.intent_id))?.as_ref()
        != Some(binding)
    {
        return Ok(Some("source_sell_binding_conflict"));
    }
    let Some(staged) = source_sell_intent_rows::load(conn, &binding.intent_id)? else {
        return Ok(Some("source_sell_staged_missing"));
    };
    let canonical = rows::signal(&staged)?;
    if canonical.signal_id != binding.signal_id {
        return Ok(Some("source_sell_binding_conflict"));
    }
    let Some(saved) = store.load_copy_signal_by_signal_id(&binding.signal_id)? else {
        return Ok(Some("source_sell_signal_missing"));
    };
    if !rows::same_identity(&saved, &canonical)
        || !rows::same_identity(caller, &saved)
        || caller.status != saved.status
    {
        return Ok(Some("source_sell_signal_conflict"));
    }
    if saved.status != EXECUTION_SELL_INTENT_STATUS {
        return Ok(Some("source_sell_signal_not_pending"));
    }
    Ok(
        source_sell_validation::revalidate(store, conn, &staged)?.map(|r| match r {
            Reject::InvalidSell => "source_sell_invalid_event",
            Reject::ObservedEventMismatch => "source_sell_observed_mismatch",
            Reject::NoOwnedPosition => "source_sell_no_owned_position",
            Reject::GenerationMismatch => "source_sell_generation_mismatch",
            Reject::SellBeforePosition => "source_sell_before_position",
            Reject::SellBeforeLatestBuy => "source_sell_before_latest_buy",
            Reject::ShadowRiskPresent => "source_sell_shadow_risk_present",
            Reject::SourceNotProven | Reject::WitnessNoLongerProven => {
                "source_sell_witness_not_proven"
            }
            Reject::SignalAlreadyExists | Reject::StagedEventConflict => {
                "source_sell_identity_conflict"
            }
        }),
    )
}
