//! Preserve the checked order/source identity across an amount refusal.
use super::{Refusal, Snapshot};
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{ExecutionCanaryOrder, SqliteStore};

#[derive(Debug, Clone)]
pub(super) struct Identity {
    order: ExecutionCanaryOrder,
    signal: CopySignalRow,
}
impl PartialEq for Identity {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order && super::same_signal(&self.signal, &other.signal)
    }
}
impl Eq for Identity {}

pub(crate) fn attach(error: anyhow::Error, state: Option<&Snapshot>) -> anyhow::Error {
    match error.downcast::<Refusal>() {
        Ok(mut refusal) => {
            if refusal.reason == "source_sell_amount_stale" {
                if let Some((s, order)) = state.and_then(|s| s.order.as_ref().map(|o| (s, o))) {
                    refusal.id = order.order_id.clone();
                    refusal.order_id = Some(order.order_id.clone());
                    refusal.retry = Some(Box::new(Identity {
                        order: order.clone(),
                        signal: s.signal.clone(),
                    }));
                }
            }
            refusal.into()
        }
        Err(error) => error,
    }
}

pub(crate) fn retire(store: &SqliteStore, now: DateTime<Utc>, refusal: &Refusal) -> Result<()> {
    if refusal.reason == "source_sell_amount_stale" {
        if let Some(identity) = &refusal.retry {
            store.mark_execution_canary_stale_sell_amount_failed(
                &identity.order,
                &identity.signal,
                now,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn state_result<T>(
    store: &SqliteStore,
    now: DateTime<Utc>,
    result: Result<T>,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<Option<T>> {
    if let Err(error) = &result {
        if let Some(refusal) = error.downcast_ref::<Refusal>() {
            retire(store, now, refusal)?;
        }
    }
    super::state_result(result, summary)
}
