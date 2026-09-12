use crate::{
    source_sell_event as event, source_sell_intent_rows as rows, ExecutionSourceSellIntent,
    ExecutionSourceSellOutcome as Outcome, ExecutionSourceSellReject as Reject,
    SqliteDiscoveryStore,
};
use anyhow::{Context, Result};
use chrono::Utc;
use copybot_core_types::SwapEvent;

impl SqliteDiscoveryStore {
    /// Stage a source event for one exact OPEN generation. This method owns only
    /// execution_source_sell_intents; it never creates a runnable signal/order.
    /// The separate promotion API must revalidate source/generation atomically.
    pub fn stage_execution_source_sell_intent(
        &self,
        swap: &SwapEvent,
        expected_position_id: &str,
    ) -> Result<Outcome> {
        self.with_immediate_transaction_retry("stage source SELL intent", |conn| {
            stage_on_conn(self, conn, swap, expected_position_id)
        })
        .context("prepare source-bound SELL")
    }

    /// Bounded immutable history lookup. This does not grant fresh authorization;
    /// in particular it can return a record for a CLOSED generation after retention.
    pub fn load_execution_source_sell_intent(
        &self,
        intent_id: &str,
    ) -> Result<Option<ExecutionSourceSellIntent>> {
        rows::load(&self.conn, intent_id)
    }
}

// Caller owns the IMMEDIATE transaction; authority checks are shared unchanged.
pub(crate) fn stage_on_conn(
    store: &SqliteDiscoveryStore,
    conn: &rusqlite::Connection,
    swap: &SwapEvent,
    expected_position_id: &str,
) -> Result<Outcome> {
    use Reject::*;
    let reject = |reason| Ok(Outcome::Rejected(reason));
    if !event::valid(swap) {
        return reject(InvalidSell);
    }
    if crate::ordered_source_sell::ownership::blocks_legacy(conn, &swap.signature)? {
        return reject(StagedEventConflict);
    }
    if !event::matches_observed(conn, swap)? {
        return reject(ObservedEventMismatch);
    }
    let id = event::intent_id(&swap.signature);
    let existing = rows::load(conn, &id)?;
    if let Some(old) = &existing {
        if !event::same(&old.event, swap) {
            return reject(StagedEventConflict);
        }
        if old.position_id != expected_position_id {
            return reject(GenerationMismatch);
        }
    }
    if let Some(reason) =
        crate::source_sell_validation::position(store, conn, swap, expected_position_id)?
    {
        return reject(reason);
    }
    if conn
        .prepare("SELECT 1 FROM copy_signals WHERE signal_id=?1")?
        .exists([event::signal_id(swap)])?
    {
        return reject(SignalAlreadyExists);
    }
    let (witness, execution_wallet) = match crate::source_sell_validation::witness(
        conn,
        swap,
        expected_position_id,
        existing.as_ref(),
    )? {
        Ok(proof) => proof,
        Err(reason) => return reject(reason),
    };
    if let Some(old) = existing {
        return Ok(Outcome::Existing(old));
    }
    let staged = ExecutionSourceSellIntent {
        intent_id: id,
        event: swap.clone(),
        position_id: expected_position_id.to_owned(),
        buy_witness: witness,
        buy_execution_wallet: execution_wallet,
        staged_at: Utc::now(),
    };
    rows::insert(conn, &staged)?;
    Ok(Outcome::Inserted(staged))
}
