use super::{record_confirmed_fill_accounting, ExecutionConfirmedFill};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    SqliteStore, EXECUTION_STATUS_CANARY_CONFIRMED, EXECUTION_STATUS_CANARY_SUBMITTED,
};

pub(crate) const NO_SEND_CONFIRMATION_TRACKER_REASON: &str = "confirmation_tracker_no_send";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionConfirmationRequest {
    pub(crate) order_id: String,
    pub(crate) tx_signature: String,
    pub(crate) requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionConfirmationProof {
    pub(crate) tx_signature: String,
    pub(crate) confirmation_status: String,
    pub(crate) slot: Option<u64>,
    pub(crate) confirmed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExecutionConfirmationTrackerOutcome {
    Pending {
        tx_signature: String,
        reason: String,
    },
    Confirmed(ExecutionConfirmationProof),
}

pub(crate) trait ExecutionConfirmationTracker {
    fn check_confirmation(
        &self,
        request: &ExecutionConfirmationRequest,
    ) -> Result<ExecutionConfirmationTrackerOutcome>;
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NoSendExecutionConfirmationTracker;

impl ExecutionConfirmationTracker for NoSendExecutionConfirmationTracker {
    fn check_confirmation(
        &self,
        request: &ExecutionConfirmationRequest,
    ) -> Result<ExecutionConfirmationTrackerOutcome> {
        validate_confirmation_request(request)?;
        Ok(ExecutionConfirmationTrackerOutcome::Pending {
            tx_signature: request.tx_signature.clone(),
            reason: NO_SEND_CONFIRMATION_TRACKER_REASON.to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MockConfirmedExecutionConfirmationTracker {
    pub(crate) slot: Option<u64>,
    pub(crate) confirmation_status: String,
    pub(crate) confirmed_at: DateTime<Utc>,
}

impl ExecutionConfirmationTracker for MockConfirmedExecutionConfirmationTracker {
    fn check_confirmation(
        &self,
        request: &ExecutionConfirmationRequest,
    ) -> Result<ExecutionConfirmationTrackerOutcome> {
        validate_confirmation_request(request)?;
        if self.confirmation_status.trim().is_empty() {
            anyhow::bail!("confirmation status must be non-empty");
        }
        Ok(ExecutionConfirmationTrackerOutcome::Confirmed(
            ExecutionConfirmationProof {
                tx_signature: request.tx_signature.clone(),
                confirmation_status: self.confirmation_status.clone(),
                slot: self.slot,
                confirmed_at: self.confirmed_at,
            },
        ))
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionConfirmationTrackerRecordOutcome {
    pub(crate) confirmed: usize,
    pub(crate) pending: usize,
    pub(crate) reason: Option<String>,
    pub(crate) confirmation_status: Option<String>,
    pub(crate) slot: Option<u64>,
    pub(crate) fill_accounting:
        Option<super::confirmed_fill::ExecutionConfirmedFillAccountingOutcome>,
}

pub(crate) fn build_confirmation_request_from_order(
    store: &SqliteStore,
    order_id: &str,
    requested_at: DateTime<Utc>,
) -> Result<ExecutionConfirmationRequest> {
    let order = store
        .load_execution_canary_order(order_id)?
        .ok_or_else(|| anyhow!("missing execution canary order {order_id}"))?;
    if order.status != EXECUTION_STATUS_CANARY_SUBMITTED {
        anyhow::bail!(
            "execution confirmation request requires submitted order {order_id}, got {}",
            order.status
        );
    }
    let tx_signature = order
        .tx_signature
        .filter(|signature| !signature.trim().is_empty())
        .ok_or_else(|| anyhow!("execution confirmation request requires tx_signature"))?;
    let request = ExecutionConfirmationRequest {
        order_id: order.order_id,
        tx_signature,
        requested_at,
    };
    validate_confirmation_request(&request)?;
    Ok(request)
}

pub(crate) fn record_confirmation_tracker_outcome(
    store: &SqliteStore,
    request: &ExecutionConfirmationRequest,
    outcome: ExecutionConfirmationTrackerOutcome,
    fill: ExecutionConfirmedFill,
) -> Result<ExecutionConfirmationTrackerRecordOutcome> {
    validate_confirmation_request(request)?;
    validate_fill_matches_confirmation(request, &fill)?;
    match outcome {
        ExecutionConfirmationTrackerOutcome::Pending {
            tx_signature,
            reason,
        } => {
            validate_confirmation_signature(request, &tx_signature)?;
            Ok(ExecutionConfirmationTrackerRecordOutcome {
                pending: 1,
                reason: Some(reason),
                ..ExecutionConfirmationTrackerRecordOutcome::default()
            })
        }
        ExecutionConfirmationTrackerOutcome::Confirmed(proof) => {
            validate_confirmation_signature(request, &proof.tx_signature)?;
            let order =
                store.mark_execution_canary_confirmed(&request.order_id, proof.confirmed_at)?;
            let fill_accounting = record_confirmed_fill_accounting(store, fill)?;
            Ok(ExecutionConfirmationTrackerRecordOutcome {
                confirmed: usize::from(order.status == EXECUTION_STATUS_CANARY_CONFIRMED),
                confirmation_status: Some(proof.confirmation_status),
                slot: proof.slot,
                fill_accounting: Some(fill_accounting),
                ..ExecutionConfirmationTrackerRecordOutcome::default()
            })
        }
    }
}

fn validate_confirmation_request(request: &ExecutionConfirmationRequest) -> Result<()> {
    if request.order_id.trim().is_empty() {
        anyhow::bail!("confirmation request order_id must be non-empty");
    }
    if request.tx_signature.trim().is_empty() {
        anyhow::bail!("confirmation request tx_signature must be non-empty");
    }
    Ok(())
}

fn validate_confirmation_signature(
    request: &ExecutionConfirmationRequest,
    tx_signature: &str,
) -> Result<()> {
    if tx_signature != request.tx_signature {
        anyhow::bail!(
            "confirmation tx_signature mismatch: actual={tx_signature} expected={}",
            request.tx_signature
        );
    }
    Ok(())
}

fn validate_fill_matches_confirmation(
    request: &ExecutionConfirmationRequest,
    fill: &ExecutionConfirmedFill,
) -> Result<()> {
    if fill.order_id() != request.order_id {
        anyhow::bail!(
            "confirmed fill order mismatch: actual={} expected={}",
            fill.order_id(),
            request.order_id
        );
    }
    Ok(())
}
