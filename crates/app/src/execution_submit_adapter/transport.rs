use super::ExecutionSubmitIntent;
use anyhow::Result;
use chrono::{DateTime, Utc};

pub(crate) const DEFAULT_NO_SEND_SUBMIT_TIMEOUT_MS: u64 = 3_000;
pub(crate) const NO_SEND_SUBMIT_TRANSPORT_REASON: &str = "submit_transport_no_send";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionSubmitTransportAttempt {
    pub(crate) idempotency_key: String,
    pub(crate) submit_route: String,
    pub(crate) signed_transaction_base64: String,
    pub(crate) tx_signature_hint: Option<String>,
    pub(crate) timeout_ms: u64,
    pub(crate) requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExecutionSubmitTransportOutcome {
    NotSent {
        idempotency_key: String,
        reason: String,
    },
    SubmittedUnknown {
        idempotency_key: String,
        tx_signature: Option<String>,
    },
}

impl ExecutionSubmitTransportOutcome {
    pub(crate) fn idempotency_key(&self) -> &str {
        match self {
            Self::NotSent {
                idempotency_key, ..
            } => idempotency_key,
            Self::SubmittedUnknown {
                idempotency_key, ..
            } => idempotency_key,
        }
    }

    pub(crate) fn reason_label(&self) -> &str {
        match self {
            Self::NotSent { reason, .. } => reason,
            Self::SubmittedUnknown { .. } => "submitted_unknown",
        }
    }
}

pub(crate) trait ExecutionSubmitTransport {
    fn submit(
        &self,
        attempt: &ExecutionSubmitTransportAttempt,
    ) -> Result<ExecutionSubmitTransportOutcome>;
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NoSendExecutionSubmitTransport;

impl ExecutionSubmitTransport for NoSendExecutionSubmitTransport {
    fn submit(
        &self,
        attempt: &ExecutionSubmitTransportAttempt,
    ) -> Result<ExecutionSubmitTransportOutcome> {
        validate_submit_transport_attempt(attempt)?;
        Ok(ExecutionSubmitTransportOutcome::NotSent {
            idempotency_key: attempt.idempotency_key.clone(),
            reason: NO_SEND_SUBMIT_TRANSPORT_REASON.to_string(),
        })
    }
}

pub(crate) fn dry_run_no_send_submit_intent(
    intent: &ExecutionSubmitIntent,
    now: DateTime<Utc>,
) -> Result<ExecutionSubmitTransportOutcome> {
    let attempt = build_submit_transport_attempt(intent, DEFAULT_NO_SEND_SUBMIT_TIMEOUT_MS, now)?;
    NoSendExecutionSubmitTransport.submit(&attempt)
}

pub(crate) fn build_submit_transport_attempt(
    intent: &ExecutionSubmitIntent,
    timeout_ms: u64,
    requested_at: DateTime<Utc>,
) -> Result<ExecutionSubmitTransportAttempt> {
    let attempt = ExecutionSubmitTransportAttempt {
        idempotency_key: intent.idempotency_key.clone(),
        submit_route: intent.submit_route.clone(),
        signed_transaction_base64: intent.signed_transaction_base64.clone(),
        tx_signature_hint: intent.tx_signature_hint.clone(),
        timeout_ms,
        requested_at,
    };
    validate_submit_transport_attempt(&attempt)?;
    Ok(attempt)
}

fn validate_submit_transport_attempt(attempt: &ExecutionSubmitTransportAttempt) -> Result<()> {
    if attempt.idempotency_key.trim().is_empty() {
        anyhow::bail!("submit transport idempotency key must be non-empty");
    }
    if attempt.submit_route.trim().is_empty() {
        anyhow::bail!("submit transport route must be non-empty");
    }
    if attempt.signed_transaction_base64.trim().is_empty() {
        anyhow::bail!("submit transport signed transaction must be non-empty");
    }
    if attempt.timeout_ms == 0 {
        anyhow::bail!("submit transport timeout must be positive");
    }
    if attempt.timeout_ms > 60_000 {
        anyhow::bail!("submit transport timeout exceeds max 60000ms");
    }
    if attempt
        .tx_signature_hint
        .as_deref()
        .is_some_and(|hint| hint.trim().is_empty())
    {
        anyhow::bail!("submit transport transaction signature hint must be non-empty");
    }
    Ok(())
}
