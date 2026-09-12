//! Fresh durable source checks at quote, simulation and signing boundaries.
#[path = "execution_owned_sell_amount.rs"]
pub(crate) mod amount;
#[path = "execution_source_sell_retry.rs"]
pub(crate) mod retry;
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_submit_adapter::ExecutionSubmitRequest;
use anyhow::Result;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{ExecutionCanaryOrder, ExecutionSourceSellGuard as Check, SqliteStore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Refusal {
    pub(crate) id: String,
    pub(crate) order_id: Option<String>,
    pub(crate) reason: &'static str,
    retry: Option<Box<retry::Identity>>,
}
impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: id={}", self.reason, self.id)
    }
}
impl std::error::Error for Refusal {}
fn refused(id: &str, reason: &'static str) -> anyhow::Error {
    Refusal {
        id: id.into(),
        order_id: None,
        reason,
        retry: None,
    }
    .into()
}
pub(crate) fn on_order(error: anyhow::Error, id: &str) -> anyhow::Error {
    match error.downcast::<Refusal>() {
        Ok(mut r) => {
            r.id = id.into();
            r.order_id = Some(id.into());
            r.into()
        }
        Err(error) => error,
    }
}

pub(crate) struct Snapshot {
    pub(crate) signal: CopySignalRow,
    promoted: bool,
    order: Option<ExecutionCanaryOrder>,
    amount: Option<amount::Proof>,
}
pub(crate) fn signal(store: &SqliteStore, id: &str) -> Result<Snapshot> {
    let checked = store.check_execution_source_sell(id).map_err(|error| {
        if crate::execution_canary_summary::is_local_source_write_off_error(&error) {
            refused(id, "source_sell_state_unavailable")
        } else {
            error
        }
    })?;
    let (signal, promoted) = match checked {
        Check::NotPromoted(signal) => (signal, false),
        Check::Allowed(signal) => (signal, true),
        Check::Refused(reason) => return Err(refused(id, reason)),
    };
    Ok(Snapshot {
        signal,
        promoted,
        order: None,
        amount: None,
    })
}

/// None preserves actual legacy BUY semantics; a caller BUY flag cannot bypass a durable SELL.
pub(crate) fn order(store: &SqliteStore, id: &str, statuses: &[&str]) -> Result<Option<Snapshot>> {
    let result = (|| {
        let order = store
            .load_execution_canary_order(id)?
            .ok_or_else(|| refused(id, "source_sell_order_missing"))?;
        let mut state = signal(store, &order.signal_id)?;
        if !state.promoted && !state.signal.side.eq_ignore_ascii_case("sell") {
            return Ok(None);
        }
        if !statuses.contains(&order.status.as_str()) || known_signature(&order) {
            return Err(refused(id, "source_sell_order_not_eligible"));
        }
        if let Some(reason) = store.execution_canary_receipt_submit_block_reason(
            id,
            &state.signal.token,
            &state.signal.side,
        )? {
            // Receipt rejection stays bounded and cannot become a local signing failure.
            let _ = reason;
            return Err(refused(id, "source_sell_receipt_pending"));
        }
        state.order = Some(order);
        Ok(Some(state))
    })();
    result.map_err(|e| on_order(e, id))
}
pub(crate) fn request(
    store: &SqliteStore,
    r: &ExecutionSubmitRequest,
    statuses: &[&str],
) -> Result<Option<Snapshot>> {
    let mut state = order(store, &r.order_id, statuses)?;
    if state.is_none() && r.side.eq_ignore_ascii_case("sell") {
        return Err(on_order(
            refused(&r.order_id, "source_sell_order_identity_mismatch"),
            &r.order_id,
        ));
    }
    if let Some(state) = &mut state {
        let order = state.order.as_ref().expect("order snapshot");
        if r.signal_id != order.signal_id
            || r.client_order_id != order.client_order_id
            || r.attempt != order.attempt
            || r.route != order.route
            || r.wallet_id != state.signal.wallet_id
            || r.token != state.signal.token
            || !r.side.eq_ignore_ascii_case(&state.signal.side)
        {
            return Err(on_order(
                refused(&r.order_id, "source_sell_order_identity_mismatch"),
                &r.order_id,
            ));
        }
        state.amount = Some(amount::request(store, r).map_err(|e| retry::attach(e, Some(state)))?);
    }
    Ok(state)
}
pub(crate) fn signing(
    store: &SqliteStore,
    r: &ExecutionSubmitRequest,
    p: &crate::execution_submit_adapter::ExecutionTransactionPlan,
) -> Result<()> {
    if request(
        store,
        r,
        &[copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED],
    )?
    .is_some()
        && (p.order_id != r.order_id
            || p.signal_id != r.signal_id
            || p.client_order_id != r.client_order_id
            || p.attempt != r.attempt
            || p.route != r.route
            || p.token != r.token
            || !p.side.eq_ignore_ascii_case(&r.side)
            || p.wallet_pubkey != r.wallet_pubkey
            || p.metadata != r.metadata
            || p.swap_blueprint.as_ref().is_some_and(|b| {
                Some(b.input_amount_raw.as_str()) != r.metadata.quote_in_amount_raw.as_deref()
                    || b.input_mint != r.token
            }))
    {
        return Err(on_order(
            refused(&r.order_id, "source_sell_plan_identity_mismatch"),
            &r.order_id,
        ));
    }
    Ok(())
}
pub(crate) fn global_storage_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|c| c.downcast_ref::<rusqlite::Error>().is_some())
        && !crate::execution_canary_summary::is_local_source_write_off_error(error)
}
pub(crate) fn existing(
    store: &SqliteStore,
    old: &ExecutionCanaryOrder,
    statuses: &[&str],
) -> Result<Option<Snapshot>> {
    let state = order(store, &old.order_id, statuses)?;
    if state
        .as_ref()
        .is_some_and(|s| s.order.as_ref() != Some(old))
    {
        return Err(on_order(
            refused(&old.order_id, "source_sell_order_changed"),
            &old.order_id,
        ));
    }
    Ok(state)
}
impl Snapshot {
    pub(crate) fn recheck(&self, store: &SqliteStore) -> Result<()> {
        if !self.promoted && self.order.is_none() {
            return Ok(());
        }
        let result = (|| {
            let next = signal(store, &self.signal.signal_id)?;
            if next.promoted != self.promoted || !same_signal(&self.signal, &next.signal) {
                return Err(refused(&self.signal.signal_id, "source_sell_state_changed"));
            }
            if let Some(old) = &self.order {
                if store.load_execution_canary_order(&old.order_id)?.as_ref() != Some(old) {
                    return Err(refused(&old.order_id, "source_sell_order_changed"));
                }
                if store
                    .execution_canary_receipt_submit_block_reason(
                        &old.order_id,
                        &next.signal.token,
                        &next.signal.side,
                    )?
                    .is_some()
                {
                    return Err(refused(&old.order_id, "source_sell_receipt_pending"));
                }
            }
            if let Some(amount) = &self.amount {
                amount.recheck(store)?;
            }
            Ok(())
        })();
        result
            .map_err(|e| retry::attach(e, Some(self)))
            .map_err(|e| match &self.order {
                Some(o) => on_order(e, &o.order_id),
                None => e,
            })
    }
}
pub(crate) fn recheck(state: Option<&Snapshot>, store: &SqliteStore) -> Result<()> {
    state.map_or(Ok(()), |s| s.recheck(store))
}
pub(crate) fn known_signature(order: &ExecutionCanaryOrder) -> bool {
    order
        .tx_signature
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty())
}
pub(crate) fn state_result<T>(
    result: Result<T>,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) => match error.downcast::<Refusal>() {
            Ok(r) => {
                record_state(&r, summary);
                Ok(None)
            }
            Err(error) => Err(error),
        },
    }
}
pub(crate) fn record_state(r: &Refusal, summary: &mut ExecutionCanaryStateMachineSummary) {
    summary.source_sell_refusals.record(&r.id, r.reason);
    summary.skipped_reason = Some(r.reason);
    if r.order_id.is_some() {
        summary.last_order_id = r.order_id.clone();
    }
    summary.last_error = Some(r.to_string());
}
pub(crate) fn quote_result<T>(
    result: Result<T>,
    summary: &mut crate::execution_quote_canary::ExecutionQuoteCanaryTickSummary,
) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) => match error.downcast::<Refusal>() {
            Ok(r) => {
                summary.source_sell_refusals.record(&r.id, r.reason);
                summary.decision_unknown += 1;
                summary.last_error = Some(r.to_string());
                Ok(None)
            }
            Err(error) => Err(error),
        },
    }
}
fn same_signal(a: &CopySignalRow, b: &CopySignalRow) -> bool {
    a.signal_id == b.signal_id
        && a.wallet_id == b.wallet_id
        && a.side == b.side
        && a.token == b.token
        && a.status == b.status
        && a.ts == b.ts
        && a.notional_sol.to_bits() == b.notional_sol.to_bits()
        && a.notional_lamports == b.notional_lamports
        && a.notional_origin == b.notional_origin
}
