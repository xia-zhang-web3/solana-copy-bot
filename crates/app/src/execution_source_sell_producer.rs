//! Periodic durable staging traversal. Each promotion revalidates the accepted proof API.
#[path = "execution_source_sell_producer_errors.rs"]
mod errors;
use anyhow::{Context, Result};
use copybot_storage_core::{
    ExecutionSourceSellPromotionOutcome as Outcome, ExecutionSourceSellPromotionReject as Reject,
    ExecutionSourceSellReject as Invalid, ExecutionSourceSellStagingVisit as Visit, SqliteStore,
};

pub(crate) const RAW_VISIT_BUDGET: usize = 32;

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Summary {
    pub(crate) visits: usize,
    pub(crate) inserted: usize,
    pub(crate) existing: usize,
    pub(crate) rejected: usize,
    pub(crate) malformed: usize,
    pub(crate) wrapped: bool,
    // One retained A identity, independent of the last successful B and later tick consumers.
    pub(crate) refusals: crate::execution_canary_summary::SourceSellWriteOffRefusals,
}

pub(crate) fn produce(store: &SqliteStore) -> Result<Summary> {
    let mut out = Summary::default();
    for _ in 0..RAW_VISIT_BUDGET {
        let visit = store.advance_execution_source_sell_staging()?;
        let (rowid, id) = match visit {
            Visit::Wrapped => {
                out.wrapped = true;
                break;
            }
            Visit::Row { rowid, intent_id } => (rowid, intent_id),
        };
        out.visits += 1;
        let Some(id) = id else {
            out.malformed += 1;
            out.refusals.record(
                &format!("staged-rowid:{rowid}"),
                "source_sell_staged_key_unavailable",
            );
            continue;
        };
        match store.promote_execution_source_sell_intent(&id) {
            Ok(Outcome::Inserted(_)) => out.inserted += 1,
            Ok(Outcome::Existing(_)) => out.existing += 1,
            Ok(Outcome::Rejected(reason)) => {
                out.rejected += 1;
                out.refusals.record(&id, reject_reason(reason));
            }
            Err(error) if errors::local_data(&error) => {
                out.malformed += 1;
                out.refusals.record(&id, "source_sell_staged_data_invalid");
            }
            Err(error) => return Err(error).with_context(|| format!("promote staged SELL {id}")),
        }
    }
    Ok(out)
}

fn reject_reason(reason: Reject) -> &'static str {
    match reason {
        Reject::StagedMissing => "source_sell_staged_missing",
        Reject::BindingConflict => "source_sell_binding_conflict",
        Reject::SignalMissing => "source_sell_signal_missing",
        Reject::SignalConflict => "source_sell_signal_conflict",
        Reject::SignalAlreadyExists => "source_sell_signal_already_exists",
        Reject::Validation(reason) => match reason {
            Invalid::InvalidSell => "source_sell_invalid_event",
            Invalid::ObservedEventMismatch => "source_sell_observed_mismatch",
            Invalid::NoOwnedPosition => "source_sell_no_owned_position",
            Invalid::GenerationMismatch => "source_sell_generation_mismatch",
            Invalid::SellBeforePosition => "source_sell_before_position",
            Invalid::SellBeforeLatestBuy => "source_sell_before_latest_buy",
            Invalid::ShadowRiskPresent => "source_sell_shadow_risk_present",
            Invalid::SignalAlreadyExists => "source_sell_signal_already_exists",
            Invalid::SourceNotProven => "source_sell_source_not_proven",
            Invalid::StagedEventConflict => "source_sell_staged_event_conflict",
            Invalid::WitnessNoLongerProven => "source_sell_witness_not_proven",
        },
    }
}
